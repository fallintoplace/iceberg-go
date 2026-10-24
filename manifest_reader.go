// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package iceberg

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strconv"

	"github.com/twmb/avro"
	"github.com/twmb/avro/atype"
	"github.com/twmb/avro/ocf"
)

func decodeV1Manifests(rd *ocf.Reader) ([]ManifestFile, error) {
	results := make([]ManifestFile, 0)
	for {
		tmp := new(manifestFileV1)
		if err := rd.Decode(tmp); err != nil {
			if errors.Is(err, io.EOF) {
				return results, nil
			}

			return nil, err
		}

		results = append(results, tmp.toFile())
	}
}

func decodeManifests[I interface {
	ManifestFile
	*T
}, T any](rd *ocf.Reader, version int) ([]ManifestFile, error) {
	results := make([]ManifestFile, 0)
	for {
		tmp := I(new(T))
		if err := rd.Decode(tmp); err != nil {
			if errors.Is(err, io.EOF) {
				return results, nil
			}

			return nil, err
		}

		tmp.setVersion(version)
		results = append(results, tmp)
	}
}

// ManifestReader reads the metadata and data from an avro manifest file.
// This type is not thread-safe; its methods should not be called from
// multiple goroutines.
type ManifestReader struct {
	rd             *ocf.Reader
	file           ManifestFile
	formatVersion  int
	isFallback     bool
	projected      bool
	content        ManifestContent
	fieldNameToID  map[string]int
	fieldIDToType  map[int]string
	fieldIDToScale map[int]int

	// The rest are lazily populated, on demand. Most readers
	// will likely only try to load the entries.
	schema              Schema
	schemaLoaded        bool
	partitionSpec       PartitionSpec
	partitionSpecLoaded bool

	// inheritRowIDs controls whether this reader should apply First Row ID inheritance
	// for v3 data manifests (spec: First Row ID Inheritance).
	inheritRowIDs bool
	// nextFirstRowID tracks the next first_row_id to assign when reading v3 data
	// manifests; used for First Row ID inheritance (null data file first_row_id
	// gets manifest's first_row_id + sum of preceding files' record_count).
	nextFirstRowID int64
}

// NewManifestReader returns a value that can read the contents of an avro manifest
// file. If the caller is interested in the manifest entries in the file, it must call
// [ManifestReader.Entries] before closing the provided reader.
func NewManifestReader(file ManifestFile, in io.Reader) (*ManifestReader, error) {
	return newManifestReader(file, in, nil)
}

// NewManifestReaderWithProjection returns a manifest reader that decodes the
// standard scan fields and optionally the column statistics selected by
// projection. Manifest metadata validation and entry inheritance are the same
// as in NewManifestReader; fields omitted by the projection retain their zero
// values in the returned DataFile. Entries returned by this reader are
// read-only planning values and must not be passed to a ManifestWriter, because
// omitted statistics cannot be recovered on rewrite. ManifestWriter and the
// DataFile Avro codec reject these values with ErrInvalidArgument.
func NewManifestReaderWithProjection(
	file ManifestFile,
	in io.Reader,
	projection ManifestEntryProjection,
) (*ManifestReader, error) {
	return newManifestReader(file, in, &projection)
}

func newManifestReader(
	file ManifestFile,
	in io.Reader,
	projection *ManifestEntryProjection,
) (*ManifestReader, error) {
	var writerSchema *avro.Schema
	var rd *ocf.Reader
	var err error
	if projection == nil {
		rd, err = ocf.NewReader(in)
	} else {
		rd, err = ocf.NewReader(in, ocf.WithReaderSchemaFunc(func(reader *ocf.Reader) (*avro.Schema, error) {
			writerSchema = reader.Schema()

			return projectedManifestEntrySchema(writerSchema, *projection)
		}))
	}
	if err != nil {
		return nil, err
	}

	metadata := rd.Metadata()
	if writerSchema == nil {
		writerSchema = rd.Schema()
	}
	sc := writerSchema

	formatVersion := 1
	// format-version is optional for v1 manifest files, so default to v1.
	if raw := metadata["format-version"]; len(raw) > 0 {
		formatVersion, err = strconv.Atoi(string(raw))
		if err != nil {
			return nil, fmt.Errorf("manifest file's 'format-version' metadata is invalid: %w", err)
		}
	}
	if err := validateManifestFormatVersion(formatVersion); err != nil {
		return nil, err
	}
	// The manifest's own metadata is authoritative for its version. A v2/v3
	// manifest list may reference older manifests so a table can be upgraded
	// in place without rewriting history; only a manifest newer than the list
	// referencing it indicates corruption.
	if formatVersion > file.Version() {
		return nil, fmt.Errorf("manifest file's 'format-version' metadata indicates version %d, which is newer than the v%d manifest list referencing it",
			formatVersion, file.Version())
	}

	var content ManifestContent
	switch contentStr := string(metadata["content"]); contentStr {
	case "":
		if formatVersion != 1 {
			return nil, fmt.Errorf("manifest file's 'content' metadata is missing and 'format-version' is %d, but the field is required for 'format-version' 2 and beyond",
				formatVersion)
		}
		// V1 manifests do not contain the 'content' field, but should be interpretted as 'data' files
		fallthrough
	case "data":
		content = ManifestContentData
	case "deletes":
		content = ManifestContentDeletes
	default:
		return nil, fmt.Errorf("manifest file's 'content' metadata is invalid, should be \"data\" or \"deletes\" but instead is %q",
			contentStr)
	}
	if content != file.ManifestContent() {
		return nil, fmt.Errorf("manifest file's 'content' metadata indicates %q, but entry from manifest list indicates %q",
			content.String(), file.ManifestContent().String())
	}

	isFallback := false
	if formatVersion == 1 {
		root := sc.Root()
		for _, f := range root.Fields {
			if f.Name == "snapshot_id" {
				if f.Type.Type != atype.Union {
					isFallback = true
				}

				break
			}
		}
	}
	fieldMaps := getFieldIDMap(sc)
	// day(...) partition values are days since the Unix epoch, but a manifest
	// may encode them either as an Avro int carrying the "date" logical type or
	// as a legacy plain Avro int. Overlay the manifest's partition spec so
	// day-transform fields are always exposed as iceberg.Date, regardless of the
	// Avro encoding used to write them.
	applyDayTransformDates(metadata["partition-spec"], fieldMaps.idToType)

	// A non-nil manifest-list first_row_id is the spec's inheritance signal and
	// is only assigned by a v3+ manifest-list writer, so it — not the manifest
	// file's own internal format-version — gates inheritance. A v1- or v2-era
	// manifest carried into an upgraded v3 table keeps its older format-version
	// internally but is assigned a first_row_id when written into the v3 list; its
	// data files must still inherit row IDs (spec: First Row ID Inheritance, "even
	// if the data file is existing"), otherwise compaction drops their lineage.
	inheritRowIDs := content == ManifestContentData &&
		file.FirstRowID() != nil
	var nextFirstRowID int64
	if inheritRowIDs {
		nextFirstRowID = *file.FirstRowID()
		if nextFirstRowID < 0 {
			return nil, fmt.Errorf("%w: first row ID must be non-negative: %d",
				ErrInvalidArgument, nextFirstRowID)
		}
	}

	return &ManifestReader{
		rd:             rd,
		file:           file,
		formatVersion:  formatVersion,
		isFallback:     isFallback,
		projected:      projection != nil,
		content:        content,
		fieldNameToID:  fieldMaps.nameToID,
		fieldIDToType:  fieldMaps.idToType,
		fieldIDToScale: fieldMaps.idToDecimalScale,
		inheritRowIDs:  inheritRowIDs,
		nextFirstRowID: nextFirstRowID,
	}, nil
}

// Close releases decoder resources associated with this manifest reader.
func (c *ManifestReader) Close() error {
	return c.rd.Close()
}

// Version returns the file's format version.
func (c *ManifestReader) Version() int {
	return c.formatVersion
}

// ManifestContent returns the type of content in the manifest file.
func (c *ManifestReader) ManifestContent() ManifestContent {
	return c.content
}

// SchemaID returns the schema ID encoded in the avro file's metadata.
func (c *ManifestReader) SchemaID() (int, error) {
	id, err := strconv.Atoi(string(c.rd.Metadata()["schema-id"]))
	if err != nil {
		return 0, fmt.Errorf("manifest file's 'schema-id' metadata is invalid: %w", err)
	}

	return id, nil
}

// Schema returns the schema encoded in the avro file's metadata.
func (c *ManifestReader) Schema() (*Schema, error) {
	if !c.schemaLoaded {
		schemaID, err := c.SchemaID()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(c.rd.Metadata()["schema"], &c.schema); err != nil {
			return nil, fmt.Errorf("manifest file's 'schema' metadata is invalid: %w", err)
		}
		c.schema.ID = schemaID
		c.schemaLoaded = true
	}

	return &c.schema, nil
}

// PartitionSpecID returns the partition spec ID encoded in the avro file's metadata.
func (c *ManifestReader) PartitionSpecID() (int, error) {
	id, err := strconv.Atoi(string(c.rd.Metadata()["partition-spec-id"]))
	if err != nil {
		return 0, fmt.Errorf("manifest file's 'partition-spec-id' metadata is invalid: %w", err)
	}
	if id != int(c.file.PartitionSpecID()) {
		return 0, fmt.Errorf("manifest file's 'partition-spec-id' metadata indicates %d, but entry from manifest list indicates %d",
			id, c.file.PartitionSpecID())
	}

	return id, nil
}

// PartitionSpec returns the partition spec encoded in the avro file's metadata.
func (c *ManifestReader) PartitionSpec() (*PartitionSpec, error) {
	if !c.partitionSpecLoaded {
		partitionSpecID, err := c.PartitionSpecID()
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(c.rd.Metadata()["partition-spec"], &c.partitionSpec.fields); err != nil {
			return nil, fmt.Errorf("manifest file's 'partition-spec' metadata is invalid: %w", err)
		}
		c.partitionSpec.id = partitionSpecID
		c.partitionSpec.initialize()
		c.partitionSpecLoaded = true
	}

	return &c.partitionSpec, nil
}

// ReadEntry reads the next manifest entry in the avro file's data.
func (c *ManifestReader) ReadEntry() (ManifestEntry, error) {
	var tmp ManifestEntry
	if c.isFallback {
		tmp = &fallbackManifestEntry{
			manifestEntry: manifestEntry{Data: &dataFile{}},
		}
	} else {
		tmp = &manifestEntry{Data: &dataFile{}}
	}

	if err := c.rd.Decode(tmp); err != nil {
		return nil, err
	}
	if c.isFallback {
		tmp = tmp.(*fallbackManifestEntry).toEntry()
	}
	df := tmp.DataFile().(*dataFile)
	df.projected = c.projected
	if err := df.normalizeFormat(); err != nil {
		return nil, err
	}
	switch tmp.Status() {
	case EntryStatusEXISTING, EntryStatusADDED, EntryStatusDELETED:
	default:
		return nil, fmt.Errorf("manifest entry has invalid status: %d", tmp.Status())
	}

	entryContent := tmp.DataFile().ContentType()
	switch entryContent {
	case EntryContentData:
		if c.content != ManifestContentData {
			return nil, errors.New("data file found in delete manifest")
		}
	case EntryContentPosDeletes, EntryContentEqDeletes:
		if c.content != ManifestContentDeletes {
			return nil, errors.New("delete file found in data manifest")
		}
	default:
		return nil, fmt.Errorf("manifest entry has invalid content: %d", entryContent)
	}
	tmp.inherit(c.file)
	// First Row ID Inheritance (spec): assign the manifest's first_row_id to data
	// files that lack one, advancing by record_count only on the files actually
	// assigned. Deleted entries consume no row IDs and are skipped — the
	// manifest-list writer reserves a manifest's id range as added+existing rows
	// (excludes deleted), so the read side must match or a live file following a
	// deleted one over-advances into the next manifest's range. Mirrors Java
	// ManifestReader.idAssigner (status != DELETED, increment inside the null check).
	if c.inheritRowIDs && tmp.Status() != EntryStatusDELETED {
		if df, ok := tmp.DataFile().(*dataFile); ok && df.FirstRowIDField == nil {
			id := c.nextFirstRowID
			count := df.Count()
			if count < 0 {
				return nil, fmt.Errorf("cannot inherit first row ID for data file %q: %w: record count must be non-negative: %d",
					df.FilePath(), ErrInvalidArgument, count)
			}
			nextFirstRowID, err := advanceRowID(id, 0, count)
			if err != nil {
				return nil, fmt.Errorf("cannot inherit first row ID for data file %q: %w", df.FilePath(), err)
			}
			df.FirstRowIDField = &id
			c.nextFirstRowID = nextFirstRowID
		}
	}
	if fieldToIDMap, ok := tmp.DataFile().(hasFieldToIDMap); ok {
		fieldToIDMap.setFieldNameToIDMap(c.fieldNameToID)
		fieldToIDMap.setFieldIDToLogicalTypeMap(c.fieldIDToType)
		fieldToIDMap.setFieldIDToDecimalScaleMap(c.fieldIDToScale)
	}

	return tmp, nil
}

// iterManifest returns an iterator that streams manifest entries from
// the provided reader without buffering them. If discardDeleted is true,
// entries whose status is "deleted" are skipped.
func iterManifest(
	m ManifestFile,
	f io.Reader,
	discardDeleted bool,
	projection *ManifestEntryProjection,
) iter.Seq2[ManifestEntry, error] {
	return func(yield func(ManifestEntry, error) bool) {
		var manifestReader *ManifestReader
		var err error
		if projection == nil {
			manifestReader, err = NewManifestReader(m, f)
		} else {
			manifestReader, err = NewManifestReaderWithProjection(m, f, *projection)
		}
		if err != nil {
			yield(nil, err)

			return
		}
		aborted := false
		defer func() {
			if cerr := manifestReader.Close(); cerr != nil && !aborted {
				yield(nil, cerr)
			}
		}()

		for {
			entry, err := manifestReader.ReadEntry()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				if !yield(nil, err) {
					aborted = true
				}

				return
			}
			if discardDeleted && entry.Status() == EntryStatusDELETED {
				continue
			}
			if !yield(entry, nil) {
				aborted = true

				return
			}
		}
	}
}

// ReadManifest reads in an avro list file and returns a slice
// of manifest entries or an error if one is encountered. If discardDeleted
// is true, the returned slice omits entries whose status is "deleted".
func ReadManifest(m ManifestFile, f io.Reader, discardDeleted bool) ([]ManifestEntry, error) {
	var results []ManifestEntry
	for entry, err := range iterManifest(m, f, discardDeleted, nil) {
		if err != nil {
			return results, err
		}
		results = append(results, entry)
	}

	return results, nil
}

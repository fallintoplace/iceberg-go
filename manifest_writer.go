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
	"slices"
	"strconv"

	"github.com/apache/iceberg-go/internal"
	"github.com/twmb/avro/ocf"
)

type writerImpl interface {
	prepareEntry(*manifestEntry, int64) (ManifestEntry, error)
}

type v1writerImpl struct{}

func (v1writerImpl) prepareEntry(entry *manifestEntry, sn int64) (ManifestEntry, error) {
	if entry.Snapshot != nil && *entry.Snapshot != sn {
		if entry.EntryStatus != EntryStatusEXISTING {
			return nil, fmt.Errorf("mismatched snapshot id for entry: %d vs %d", *entry.Snapshot, sn)
		}
		sn = *entry.Snapshot
	}

	return &fallbackManifestEntry{
		manifestEntry: *entry,
		Snapshot:      sn,
	}, nil
}

type v2writerImpl struct{}

func (v2writerImpl) prepareEntry(entry *manifestEntry, snapshotID int64) (ManifestEntry, error) {
	if entry.SeqNum == nil {
		if entry.Snapshot != nil && *entry.Snapshot != snapshotID {
			return nil, fmt.Errorf("found unassigned sequence number for entry from snapshot: %d", entry.SnapshotID())
		}

		if entry.EntryStatus != EntryStatusADDED {
			return nil, errors.New("only entries with status ADDED can be missing a sequence number")
		}
	}

	return entry, nil
}

type v3writerImpl struct{}

func (v3writerImpl) prepareEntry(entry *manifestEntry, snapshotID int64) (ManifestEntry, error) {
	if entry.SeqNum == nil {
		if entry.Snapshot != nil && *entry.Snapshot != snapshotID {
			return nil, fmt.Errorf("found unassigned sequence number for entry from snapshot: %d", entry.SnapshotID())
		}
		if entry.EntryStatus != EntryStatusADDED {
			return nil, errors.New("only entries with status ADDED can be missing a sequence number")
		}
	}

	return entry, nil
}

type ManifestWriter struct {
	closed  bool
	version int
	impl    writerImpl

	output io.Writer
	writer *ocf.Writer

	spec    PartitionSpec
	schema  *Schema
	content ManifestContent

	partFieldNameToID map[string]int
	partFieldIDToType map[int]string

	snapshotID    int64
	addedFiles    int32
	addedRows     int64
	existingFiles int32
	existingRows  int64
	deletedFiles  int32
	deletedRows   int64

	partitions  []map[int]any
	minSeqNum   int64
	reusedEntry manifestEntry
}

type ManifestWriterOption func(w *ManifestWriter)

func WithManifestWriterContent(content ManifestContent) ManifestWriterOption {
	return func(w *ManifestWriter) {
		w.content = content
	}
}

func NewManifestWriter(version int, out io.Writer, spec PartitionSpec, schema *Schema, snapshotID int64, opts ...ManifestWriterOption) (*ManifestWriter, error) {
	var impl writerImpl

	switch version {
	case 1:
		impl = v1writerImpl{}
	case 2:
		impl = v2writerImpl{}
	case 3:
		impl = v3writerImpl{}
	default:
		return nil, fmt.Errorf("unsupported manifest version: %d", version)
	}

	sc, err := partitionTypeToAvroSchema(spec.PartitionType(schema))
	if err != nil {
		return nil, err
	}

	fileSchema, err := internal.NewManifestEntrySchema(sc, version)
	if err != nil {
		return nil, err
	}

	nameToID, idToType, _ := getFieldIDMap(fileSchema)

	w := &ManifestWriter{
		impl:              impl,
		version:           version,
		output:            out,
		spec:              spec,
		content:           ManifestContentData,
		schema:            schema,
		partFieldNameToID: nameToID,
		partFieldIDToType: idToType,
		snapshotID:        snapshotID,
		minSeqNum:         -1,
		partitions:        make([]map[int]any, 0),
	}

	for _, apply := range opts {
		apply(w)
	}
	if version < 2 && w.content != ManifestContentData {
		return nil, fmt.Errorf("unsupported content '%s' for format version '%d'", w.content, version)
	}

	md, err := w.meta()
	if err != nil {
		return nil, err
	}

	wr, err := ocf.NewWriter(out, fileSchema,
		ocf.WithSchema(fileSchema.String()),
		ocf.WithMetadata(md),
		ocf.WithCodec(ocf.DeflateCodec(-1)))

	w.writer = wr

	return w, err
}

func (w *ManifestWriter) Close() error {
	if w.closed {
		return nil
	}

	if w.addedFiles+w.existingFiles+w.deletedFiles == 0 {
		return errors.New("empty manifest file has been written")
	}

	w.closed = true

	return w.writer.Close()
}

type ManifestFileOption func(mf *manifestFile)

// WithManifestFileContent overrides the ManifestContent of a new manifest file with the provided value
// Default: ManifestContentData
func WithManifestFileContent(content ManifestContent) ManifestFileOption {
	return func(mf *manifestFile) {
		mf.Content = content
	}
}

func (w *ManifestWriter) ToManifestFile(location string, length int64, opts ...ManifestFileOption) (ManifestFile, error) {
	if err := w.Close(); err != nil {
		return nil, err
	}

	partitions, err := constructPartitionSummaries(w.spec, w.schema, w.partitions)
	if err != nil {
		return nil, err
	}

	mf := manifestFile{
		version:            w.version,
		Path:               location,
		Len:                length,
		SpecID:             int32(w.spec.id),
		Content:            ManifestContentData,
		SeqNumber:          -1,
		MinSeqNumber:       w.minSeqNum,
		AddedSnapshotID:    w.snapshotID,
		AddedFilesCount:    w.addedFiles,
		ExistingFilesCount: w.existingFiles,
		DeletedFilesCount:  w.deletedFiles,
		AddedRowsCount:     w.addedRows,
		ExistingRowsCount:  w.existingRows,
		DeletedRowsCount:   w.deletedRows,
		PartitionList:      &partitions,
		Key:                nil,
	}
	for _, apply := range opts {
		apply(&mf)
	}

	return &mf, nil
}

func (w *ManifestWriter) meta() (map[string][]byte, error) {
	schemaJson, err := json.Marshal(w.schema)
	if err != nil {
		return nil, err
	}

	specFields := w.spec.fields

	if specFields == nil {
		specFields = []PartitionField{}
	}

	specFieldsJson, err := json.Marshal(specFields)
	if err != nil {
		return nil, err
	}

	return map[string][]byte{
		"schema":            schemaJson,
		"schema-id":         []byte(strconv.Itoa(w.schema.ID)),
		"partition-spec":    specFieldsJson,
		"partition-spec-id": []byte(strconv.Itoa(w.spec.ID())),
		"format-version":    []byte(strconv.Itoa(w.version)),
		"content":           []byte(w.content.String()),
	}, nil
}

func (w *ManifestWriter) addEntry(entry *manifestEntry) error {
	if w.closed {
		return errors.New("cannot add entry to closed manifest writer")
	}

	status := entry.Status()
	switch status {
	case EntryStatusADDED, EntryStatusEXISTING, EntryStatusDELETED:
	default:
		return fmt.Errorf("unknown entry status: %v", status)
	}
	count := entry.DataFile().Count()
	partition := entry.Data.Partition()

	entryToEncode := *entry
	if dataFile, ok := entry.DataFile().(*dataFile); ok {
		encodeDataFile := cloneDataFileAvroFields(dataFile)
		encodeDataFile.PartitionData = avroEncodePartitionData(partition, w.partFieldNameToID, w.partFieldIDToType)
		entryToEncode.Data = encodeDataFile
	}

	toEncode, err := w.impl.prepareEntry(&entryToEncode, w.snapshotID)
	if err != nil {
		return err
	}

	if err := w.writer.Encode(toEncode); err != nil {
		return err
	}

	switch status {
	case EntryStatusADDED:
		w.addedFiles++
		w.addedRows += count
	case EntryStatusEXISTING:
		w.existingFiles++
		w.existingRows += count
	case EntryStatusDELETED:
		w.deletedFiles++
		w.deletedRows += count
	}

	w.partitions = append(w.partitions, partition)

	if status == EntryStatusADDED || status == EntryStatusEXISTING {
		if seq := entry.SequenceNum(); seq >= 0 && (w.minSeqNum < 0 || seq < w.minSeqNum) {
			w.minSeqNum = seq
		}
	}

	return nil
}

func (w *ManifestWriter) Add(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusADDED, &w.snapshotID, entry.(*manifestEntry).SeqNum, nil, entry.DataFile())

	return w.addEntry(&w.reusedEntry)
}

func (w *ManifestWriter) Delete(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusDELETED, &w.snapshotID, entry.(*manifestEntry).SeqNum, entry.FileSequenceNum(), entry.DataFile())

	return w.addEntry(&w.reusedEntry)
}

func (w *ManifestWriter) Existing(entry ManifestEntry) error {
	snapshotID := entry.SnapshotID()
	w.reusedEntry.wrap(EntryStatusEXISTING, &snapshotID, entry.(*manifestEntry).SeqNum, entry.FileSequenceNum(), entry.DataFile())

	return w.addEntry(&w.reusedEntry)
}

type ManifestListWriter struct {
	version          int
	out              io.Writer
	commitSnapshotID int64
	sequenceNumber   int64
	writer           *ocf.Writer
	nextRowID        *int64
}

func NewManifestListWriterV1(out io.Writer, snapshotID int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          1,
		out:              out,
		commitSnapshotID: snapshotID,
		sequenceNumber:   -1,
	}

	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.Itoa(int(*parentSnapshot))
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.Itoa(int(snapshotID))),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func NewManifestListWriterV2(out io.Writer, snapshotID, sequenceNumber int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          2,
		out:              out,
		commitSnapshotID: snapshotID,
		sequenceNumber:   sequenceNumber,
	}

	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.Itoa(int(*parentSnapshot))
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.Itoa(int(snapshotID))),
		"sequence-number":    []byte(strconv.Itoa(int(sequenceNumber))),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func NewManifestListWriterV3(out io.Writer, snapshotId, sequenceNumber, firstRowID int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          3,
		out:              out,
		commitSnapshotID: snapshotId,
		sequenceNumber:   sequenceNumber,
		nextRowID:        &firstRowID,
	}
	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.Itoa(int(*parentSnapshot))
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.Itoa(int(snapshotId))),
		"sequence-number":    []byte(strconv.Itoa(int(sequenceNumber))),
		"first-row-id":       []byte(strconv.Itoa(int(firstRowID))),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func (m *ManifestListWriter) init(meta map[string][]byte) error {
	fileSchema, err := internal.NewManifestFileSchema(m.version)
	if err != nil {
		return err
	}

	wr, err := ocf.NewWriter(m.out, fileSchema,
		ocf.WithSchema(fileSchema.String()),
		ocf.WithMetadata(meta),
		ocf.WithCodec(ocf.DeflateCodec(-1)))
	if err != nil {
		return err
	}

	m.writer = wr

	return nil
}

func (m *ManifestListWriter) Close() error {
	if m.writer == nil {
		return nil
	}

	return m.writer.Close()
}

func (m *ManifestListWriter) NextRowID() *int64 {
	return m.nextRowID
}

func (m *ManifestListWriter) AddManifests(files []ManifestFile) error {
	if len(files) == 0 {
		return nil
	}

	switch m.version {
	case 1:
		if slices.ContainsFunc(files, func(f ManifestFile) bool {
			return f.Version() != 1
		}) {
			return fmt.Errorf("%w: ManifestListWriter only supports version 1 manifest files", ErrInvalidArgument)
		}

		var tmp manifestFileV1
		for _, file := range files {
			file.(*manifestFile).toV1(&tmp)
			if err := m.writer.Encode(&tmp); err != nil {
				return err
			}
		}

	case 2, 3:
		for _, file := range files {
			// Per the Iceberg spec a v2 manifest list may reference v1 manifest
			// files (and a v3 list may reference v1 or v2 manifests) so that a
			// table can be upgraded without rewriting historical manifests. The
			// in-memory ManifestFile produced for v1 inputs already carries the
			// inheritance values mandated by the spec — Content=data and
			// SeqNumber/MinSeqNumber=0 — so it can be encoded directly against
			// the v2/v3 entry schema. Newer-than-writer inputs are rejected
			// because the v2 schema cannot represent v3 fields such as
			// first_row_id.
			if file.Version() > m.version {
				return fmt.Errorf("%w: manifest list v%d cannot reference v%d manifest files",
					ErrInvalidArgument, m.version, file.Version())
			}

			wrapped := *(file.(*manifestFile))
			if m.version == 3 {
				// Ref: https://github.com/apache/iceberg/blob/ea2071568dc66148b483a82eefedcd2992b435f7/core/src/main/java/org/apache/iceberg/ManifestListWriter.java#L157-L168
				if wrapped.Content == ManifestContentData && wrapped.FirstRowIDValue == nil {
					if m.nextRowID != nil {
						firstRowID := *m.nextRowID
						wrapped.FirstRowIDValue = &firstRowID
						*m.nextRowID += wrapped.ExistingRowsCount + wrapped.AddedRowsCount
					}
				}
			}
			if wrapped.SeqNumber == -1 {
				// if the sequence number is being assigned here,
				// then the manifest must be created by the current
				// operation.
				// to validate this, check the snapshot id matches the current commit
				if m.commitSnapshotID != wrapped.AddedSnapshotID {
					return fmt.Errorf("found unassigned sequence number for a manifest from snapshot %d != %d",
						m.commitSnapshotID, wrapped.AddedSnapshotID)
				}
				wrapped.SeqNumber = m.sequenceNumber
			}

			if wrapped.MinSeqNumber == -1 {
				if m.commitSnapshotID != wrapped.AddedSnapshotID {
					return fmt.Errorf("found unassigned sequence number for a manifest from snapshot: %d", wrapped.AddedSnapshotID)
				}
				// if the min sequence number is not determined, then there was no assigned sequence number
				// for any file written to the wrapped manifest. replace the unassigned sequence number with
				// the one for this commit
				wrapped.MinSeqNumber = m.sequenceNumber
			}
			if err := m.writer.Encode(wrapped); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported manifest version: %d", m.version)
	}

	return nil
}

// WriteManifestList writes a list of manifest files to an avro file.
func WriteManifestList(version int, out io.Writer, snapshotID int64, parentSnapshotID, sequenceNumber *int64, firstRowId int64, files []ManifestFile) (err error) {
	var writer *ManifestListWriter

	switch version {
	case 1:
		writer, err = NewManifestListWriterV1(out, snapshotID, parentSnapshotID)
	case 2:
		if sequenceNumber == nil {
			return errors.New("sequence number is required for V2 tables")
		}
		writer, err = NewManifestListWriterV2(out, snapshotID, *sequenceNumber, parentSnapshotID)
	case 3:
		if sequenceNumber == nil {
			return errors.New("sequence number is required for V3 tables")
		}
		writer, err = NewManifestListWriterV3(out, snapshotID, *sequenceNumber, firstRowId, parentSnapshotID)
	default:
		return fmt.Errorf("unsupported manifest version: %d", version)
	}

	if err != nil {
		return err
	}
	defer internal.CheckedClose(writer, &err)

	return writer.AddManifests(files)
}

func WriteManifest(
	filename string,
	out io.Writer,
	version int,
	spec PartitionSpec,
	schema *Schema,
	snapshotID int64,
	entries []ManifestEntry,
) (mf ManifestFile, err error) {
	cnt := &internal.CountingWriter{W: out}

	w, err := NewManifestWriter(version, cnt, spec, schema, snapshotID)
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(w, &err)

	for _, entry := range entries {
		if err := w.addEntry(entry.(*manifestEntry)); err != nil {
			return nil, err
		}
	}

	// flush the writer to ensure cnt.Count is accurate
	if err := w.Close(); err != nil {
		return nil, err
	}

	return w.ToManifestFile(filename, cnt.Count)
}

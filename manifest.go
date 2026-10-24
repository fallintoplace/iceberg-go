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
	"iter"
	"slices"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/twmb/avro"
	"github.com/twmb/avro/atype"
)


// ManifestContent indicates the type of data inside of the files
// described by a manifest. This will indicate whether the data files
// contain active data or deleted rows.
type ManifestContent int32

const (
	ManifestContentData    ManifestContent = 0
	ManifestContentDeletes ManifestContent = 1
)

func (m ManifestContent) String() string {
	switch m {
	case ManifestContentData:
		return "data"
	case ManifestContentDeletes:
		return "deletes"
	default:
		return "UNKNOWN"
	}
}

const initialSequenceNumber = 0

type FieldSummary struct {
	ContainsNull bool    `avro:"contains_null"`
	ContainsNaN  *bool   `avro:"contains_nan"`
	LowerBound   *[]byte `avro:"lower_bound"`
	UpperBound   *[]byte `avro:"upper_bound"`
}

type ManifestBuilder struct {
	m *manifestFile
}

func NewManifestFile(version int, path string, length int64, partitionSpecID int32, addedSnapshotID int64) *ManifestBuilder {
	var seqNum int64
	if version != 1 {
		seqNum = -1
	}

	return &ManifestBuilder{
		m: &manifestFile{
			version:         version,
			Path:            path,
			Len:             length,
			SpecID:          partitionSpecID,
			AddedSnapshotID: addedSnapshotID,
			Content:         ManifestContentData,
			SeqNumber:       seqNum,
			MinSeqNumber:    seqNum,
		},
	}
}

func (b *ManifestBuilder) SequenceNum(num, minSeqNum int64) *ManifestBuilder {
	b.m.SeqNumber, b.m.MinSeqNumber = num, minSeqNum

	return b
}

func (b *ManifestBuilder) Content(content ManifestContent) *ManifestBuilder {
	b.m.Content = content

	return b
}

func (b *ManifestBuilder) AddedFiles(cnt int32) *ManifestBuilder {
	b.m.AddedFilesCount = cnt

	return b
}

func (b *ManifestBuilder) ExistingFiles(cnt int32) *ManifestBuilder {
	b.m.ExistingFilesCount = cnt

	return b
}

func (b *ManifestBuilder) DeletedFiles(cnt int32) *ManifestBuilder {
	b.m.DeletedFilesCount = cnt

	return b
}

func (b *ManifestBuilder) AddedRows(cnt int64) *ManifestBuilder {
	b.m.AddedRowsCount = cnt

	return b
}

func (b *ManifestBuilder) ExistingRows(cnt int64) *ManifestBuilder {
	b.m.ExistingRowsCount = cnt

	return b
}

func (b *ManifestBuilder) DeletedRows(cnt int64) *ManifestBuilder {
	b.m.DeletedRowsCount = cnt

	return b
}

func (b *ManifestBuilder) Partitions(p []FieldSummary) *ManifestBuilder {
	if p == nil {
		// Preserve the semantic "no partition summaries" state by clearing the
		// union pointer entirely instead of storing a pointer to a nil slice.
		b.m.PartitionList = nil

		return b
	}

	copied := cloneFieldSummaries(p)
	b.m.PartitionList = &copied

	return b
}

func cloneFieldSummaries(partitions []FieldSummary) []FieldSummary {
	if partitions == nil {
		return nil
	}

	copied := make([]FieldSummary, len(partitions))
	for i, partition := range partitions {
		copiedPartition := partition
		if partition.LowerBound != nil {
			lowerBound := slices.Clone(*partition.LowerBound)
			copiedPartition.LowerBound = &lowerBound
		}
		if partition.UpperBound != nil {
			upperBound := slices.Clone(*partition.UpperBound)
			copiedPartition.UpperBound = &upperBound
		}
		if partition.ContainsNaN != nil {
			containsNaN := *partition.ContainsNaN
			copiedPartition.ContainsNaN = &containsNaN
		}

		copied[i] = copiedPartition
	}

	return copied
}

func (b *ManifestBuilder) KeyMetadata(km []byte) *ManifestBuilder {
	if km == nil {
		// Preserve the semantic "no key metadata" state by clearing the union
		// pointer entirely instead of storing a pointer to a nil slice.
		b.m.Key = nil

		return b
	}

	keyMetadata := slices.Clone(km)
	b.m.Key = &keyMetadata

	return b
}

func (b *ManifestBuilder) Build() ManifestFile {
	return b.m
}

// manifestFileV1 is a read/write struct for V1 manifest list entries.
// Pointer fields handle nullable Avro types (["null", T] unions).
// The three count fields carry aliases for pre-1.4 Java Iceberg legacy names
// (apache/iceberg#5338) so that avro.Resolve maps them transparently.
type manifestFileV1 struct {
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	SpecID             int32           `avro:"partition_spec_id"`
	AddedSnapshotID    *int64          `avro:"added_snapshot_id"`
	AddedFilesCount    *int32          `avro:"added_files_count,alias=added_data_files_count"`
	ExistingFilesCount *int32          `avro:"existing_files_count,alias=existing_data_files_count"`
	DeletedFilesCount  *int32          `avro:"deleted_files_count,alias=deleted_data_files_count"`
	AddedRowsCount     *int64          `avro:"added_rows_count"`
	ExistingRowsCount  *int64          `avro:"existing_rows_count"`
	DeletedRowsCount   *int64          `avro:"deleted_rows_count"`
	PartitionList      *[]FieldSummary `avro:"partitions,type-alias=r508"`
	Key                *[]byte         `avro:"key_metadata"`
}

func (m *manifestFileV1) toFile() *manifestFile {
	snapshotID := int64(-1)
	if m.AddedSnapshotID != nil {
		snapshotID = *m.AddedSnapshotID
	}

	f := &manifestFile{
		version:         1,
		Path:            m.Path,
		Len:             m.Len,
		SpecID:          m.SpecID,
		Content:         ManifestContentData,
		SeqNumber:       initialSequenceNumber,
		MinSeqNumber:    initialSequenceNumber,
		AddedSnapshotID: snapshotID,
		PartitionList:   m.PartitionList,
		Key:             m.Key,
	}

	if m.AddedFilesCount != nil {
		f.AddedFilesCount = *m.AddedFilesCount
	} else {
		f.AddedFilesCount = -1
	}

	if m.ExistingFilesCount != nil {
		f.ExistingFilesCount = *m.ExistingFilesCount
	} else {
		f.ExistingFilesCount = -1
	}

	if m.DeletedFilesCount != nil {
		f.DeletedFilesCount = *m.DeletedFilesCount
	} else {
		f.DeletedFilesCount = -1
	}

	if m.AddedRowsCount != nil {
		f.AddedRowsCount = *m.AddedRowsCount
	} else {
		f.AddedRowsCount = -1
	}

	if m.ExistingRowsCount != nil {
		f.ExistingRowsCount = *m.ExistingRowsCount
	} else {
		f.ExistingRowsCount = -1
	}

	if m.DeletedRowsCount != nil {
		f.DeletedRowsCount = *m.DeletedRowsCount
	} else {
		f.DeletedRowsCount = -1
	}

	return f
}

type manifestFile struct {
	Path               string          `avro:"manifest_path"`
	Len                int64           `avro:"manifest_length"`
	SpecID             int32           `avro:"partition_spec_id"`
	Content            ManifestContent `avro:"content"`
	SeqNumber          int64           `avro:"sequence_number"`
	MinSeqNumber       int64           `avro:"min_sequence_number"`
	AddedSnapshotID    int64           `avro:"added_snapshot_id"`
	AddedFilesCount    int32           `avro:"added_files_count,alias=added_data_files_count"`
	ExistingFilesCount int32           `avro:"existing_files_count,alias=existing_data_files_count"`
	DeletedFilesCount  int32           `avro:"deleted_files_count,alias=deleted_data_files_count"`
	AddedRowsCount     int64           `avro:"added_rows_count"`
	ExistingRowsCount  int64           `avro:"existing_rows_count"`
	DeletedRowsCount   int64           `avro:"deleted_rows_count"`
	PartitionList      *[]FieldSummary `avro:"partitions,type-alias=r508"`
	Key                *[]byte         `avro:"key_metadata"`
	FirstRowIDValue    *int64          `avro:"first_row_id"`

	version int `avro:"-"`
}

func (m *manifestFile) setVersion(v int) {
	m.version = v
}

func (m *manifestFile) toV1(v1file *manifestFileV1) {
	v1file.Path = m.Path
	v1file.Len = m.Len
	v1file.SpecID = m.SpecID
	v1file.AddedSnapshotID = &m.AddedSnapshotID
	v1file.PartitionList = m.PartitionList
	v1file.Key = m.Key

	if m.AddedFilesCount >= 0 {
		v1file.AddedFilesCount = &m.AddedFilesCount
	} else {
		v1file.AddedFilesCount = nil
	}

	if m.ExistingFilesCount >= 0 {
		v1file.ExistingFilesCount = &m.ExistingFilesCount
	} else {
		v1file.ExistingFilesCount = nil
	}

	if m.DeletedFilesCount >= 0 {
		v1file.DeletedFilesCount = &m.DeletedFilesCount
	} else {
		v1file.DeletedFilesCount = nil
	}

	if m.AddedRowsCount >= 0 {
		v1file.AddedRowsCount = &m.AddedRowsCount
	} else {
		v1file.AddedRowsCount = nil
	}

	if m.ExistingRowsCount >= 0 {
		v1file.ExistingRowsCount = &m.ExistingRowsCount
	} else {
		v1file.ExistingRowsCount = nil
	}

	if m.DeletedRowsCount >= 0 {
		v1file.DeletedRowsCount = &m.DeletedRowsCount
	} else {
		v1file.DeletedRowsCount = nil
	}
}

// ensurePartitionList normalizes the narrow case behind #1309: an inherited
// manifest whose partition-field summary list is present but empty. Only an
// empty-spec manifest has zero summaries, so in practice this is an
// unpartitioned table. On decode that present-empty array becomes a non-nil
// pointer over a nil slice, which the manifest-list writer would otherwise
// re-encode as Avro null for the optional partitions union (field 507) — a
// value strict readers such as Redshift Spectrum reject. Only that
// present-but-nil slice is normalized back to an empty array. A genuinely
// absent value (nil pointer) is left null, which is spec-legal because
// partitions is optional; populated (partitioned) summaries are non-empty and
// pass through untouched.
func ensurePartitionList(partitions **[]FieldSummary) {
	if *partitions == nil {
		return
	}
	if **partitions == nil {
		empty := []FieldSummary{}
		*partitions = &empty
	}
}

func (m *manifestFile) Version() int                     { return m.version }
func (m *manifestFile) FilePath() string                 { return m.Path }
func (m *manifestFile) Length() int64                    { return m.Len }
func (m *manifestFile) PartitionSpecID() int32           { return m.SpecID }
func (m *manifestFile) ManifestContent() ManifestContent { return m.Content }
func (m *manifestFile) SnapshotID() int64                { return m.AddedSnapshotID }
func (m *manifestFile) AddedDataFiles() int32            { return m.AddedFilesCount }
func (m *manifestFile) ExistingDataFiles() int32         { return m.ExistingFilesCount }
func (m *manifestFile) DeletedDataFiles() int32          { return m.DeletedFilesCount }
func (m *manifestFile) AddedRows() int64                 { return m.AddedRowsCount }
func (m *manifestFile) ExistingRows() int64              { return m.ExistingRowsCount }
func (m *manifestFile) DeletedRows() int64               { return m.DeletedRowsCount }
func (m *manifestFile) SequenceNum() int64               { return m.SeqNumber }
func (m *manifestFile) MinSequenceNum() int64            { return m.MinSeqNumber }
func (m *manifestFile) KeyMetadata() []byte {
	if m.Key == nil {
		return nil
	}

	return slices.Clone(*m.Key)
}

func (m *manifestFile) Partitions() []FieldSummary {
	if m.PartitionList == nil {
		return nil
	}

	return cloneFieldSummaries(*m.PartitionList)
}

func (m *manifestFile) FirstRowID() *int64 {
	if m.FirstRowIDValue == nil {
		return nil
	}

	firstRowID := *m.FirstRowIDValue

	return &firstRowID
}

func (m *manifestFile) HasAddedFiles() bool    { return m.AddedFilesCount != 0 }
func (m *manifestFile) HasExistingFiles() bool { return m.ExistingFilesCount != 0 }

func (m *manifestFile) Entries(fs iceio.IO, discardDeleted bool) iter.Seq2[ManifestEntry, error] {
	return manifestEntries(fs, m, discardDeleted, nil)
}

func getFieldIDMap(sc *avro.Schema) dataFileFieldMaps {
	root := sc.Root()
	getField := func(node avro.SchemaNode, name string) *avro.SchemaField {
		for i := range node.Fields {
			if node.Fields[i].Name == name {
				return &node.Fields[i]
			}
		}

		return nil
	}

	result := make(map[string]int)
	logicalTypes := make(map[int]string)
	fixedSizes := make(map[int]int)
	decimalScales := make(map[int]int)
	unknownFieldIDs := make(map[int]struct{})

	entryField := getField(*root, "data_file")
	partitionField := getField(entryField.Type, "partition")

	for _, field := range partitionField.Type.Fields {
		fid, ok := schemaFieldID(field)
		if !ok {
			continue
		}

		result[field.Name] = fid
		typ := field.Type
		if typ.Type == atype.Union {
			typ = typ.Branches[len(typ.Branches)-1]
		}
		if typ.Type == atype.Null {
			unknownFieldIDs[fid] = struct{}{}
		}
		if typ.LogicalType != "" {
			logicalTypes[fid] = typ.LogicalType
			if typ.LogicalType == atype.Decimal {
				fixedSizes[fid] = typ.Size
				decimalScales[fid] = typ.Scale
			}
		}
	}

	return dataFileFieldMaps{
		nameToID:         result,
		idToType:         logicalTypes,
		idToFixedSize:    fixedSizes,
		idToDecimalScale: decimalScales,
		unknownFieldIDs:  unknownFieldIDs,
	}
}

// applyDayTransformDates marks every day(...) partition field described by the
// manifest's partition-spec metadata as a "date" logical type. This normalizes
// day-transform partition values to iceberg.Date even when they were written as
// a plain Avro int with no logical type, so callers never observe the Avro
// encoding difference. Only day transforms are affected; other plain integer
// partition values (including hour/month/year transforms) are left untouched.
//
// The Iceberg spec gives the day transform result type "date" and requires
// readers to also accept the plain-int encoding, interpreting each integer as
// the number of days since 1970-01-01 (see the transform table and note [1] in
// apache/iceberg#16446). Accepting both encodings is therefore spec-mandated,
// not merely cross-engine convention.
//
// Absent or malformed partition-spec metadata is ignored: the reader simply
// falls back to the logical types declared in the Avro schema.
func applyDayTransformDates(specJSON []byte, fieldIDToType map[int]string) {
	if len(specJSON) == 0 {
		return
	}

	var fields []PartitionField
	if err := json.Unmarshal(specJSON, &fields); err != nil {
		return
	}

	for _, f := range fields {
		if _, ok := f.Transform.(DayTransform); ok {
			fieldIDToType[f.FieldID] = atype.Date
		}
	}
}

type hasFieldToIDMap interface {
	setFieldNameToIDMap(map[string]int)
	setFieldIDToLogicalTypeMap(map[int]string)
	setFieldIDToDecimalScaleMap(map[int]int)
}

// ManifestFile is the interface for version 1, 2, and 3 manifest files.
type ManifestFile interface {
	// Version returns the version number of this manifest file.
	// It must be 1, 2, or 3.
	Version() int
	// FilePath is the location URI of this manifest file.
	FilePath() string
	// Length is the length in bytes of the manifest file.
	Length() int64
	// PartitionSpecID is the ID of the partition spec used to write
	// this manifest. It must be listed in the table metadata
	// partition-specs.
	PartitionSpecID() int32
	// ManifestContent is the type of files tracked by this manifest,
	// either data or delete files. All v1 manifests track data files.
	ManifestContent() ManifestContent
	// SnapshotID is the ID of the snapshot where this manifest file
	// was added.
	SnapshotID() int64
	// AddedDataFiles returns the number of entries in the manifest that
	// have the status of EntryStatusADDED.
	AddedDataFiles() int32
	// ExistingDataFiles returns the number of entries in the manifest
	// which have the status of EntryStatusEXISTING.
	ExistingDataFiles() int32
	// DeletedDataFiles returns the number of entries in the manifest
	// which have the status of EntryStatusDELETED.
	DeletedDataFiles() int32
	// AddedRows returns the number of rows in all files of the manifest
	// that have status EntryStatusADDED.
	AddedRows() int64
	// ExistingRows returns the number of rows in all files of the manifest
	// which have status EntryStatusEXISTING.
	ExistingRows() int64
	// DeletedRows returns the number of rows in all files of the manifest
	// which have status EntryStatusDELETED.
	DeletedRows() int64
	// SequenceNum returns the sequence number when this manifest was
	// added to the table. Will be 0 for v1 manifest lists.
	SequenceNum() int64
	// MinSequenceNum is the minimum data sequence number of all live data
	// or delete files in the manifest. Will be 0 for v1 manifest lists.
	MinSequenceNum() int64
	// KeyMetadata returns implementation-specific key metadata for encryption
	// if it exists in the manifest list.
	KeyMetadata() []byte
	// Partitions returns a list of field summaries for each partition
	// field in the spec. Each field in the list corresponds to a field in
	// the manifest file's partition spec.
	Partitions() []FieldSummary
	// FirstRowID returns the first _row_id assigned to rows in this manifest (v3+ data manifests only).
	// Returns nil for v1/v2 or for delete manifests.
	FirstRowID() *int64

	// HasAddedFiles returns true if AddedDataFiles > 0 or if it was null.
	HasAddedFiles() bool
	// HasExistingFiles returns true if ExistingDataFiles > 0 or if it was null.
	HasExistingFiles() bool
	// Entries streams the manifest entries from the manifest file using
	// the provided file system IO interface. Entries that have been
	// marked as deleted are skipped if discardDeleted is true.
	//
	// Entries avoids loading every manifest entry into memory at once,
	// making it suitable for walking large manifests.
	//
	// Iteration contract:
	//
	//   - On the first error encountered while opening the manifest, decoding
	//     a record, or applying inheritance, the iterator yields (nil, err)
	//     and then stops. Callers must treat any non-nil error as terminal and
	//     break or return without consuming further values.
	//   - When iteration ends without an error from the read path, the
	//     iterator may yield a final (nil, closeErr) pair if closing the
	//     underlying file or manifest reader returns an error. This terminal
	//     close error is reported only when the consumer ranged through every
	//     value; an early break suppresses it (see below).
	//   - Breaking out of the range loop (or any other early termination of
	//     the yield function) is safe: the iterator releases the underlying
	//     file handle and reader before returning, and no close error from
	//     that path is yielded — the caller has already signalled it is no
	//     longer interested in further values, so an extra synthetic
	//     (nil, closeErr) tail would be discarded anyway.
	Entries(fs iceio.IO, discardDeleted bool) iter.Seq2[ManifestEntry, error]
	// // WriteEntries writes a list of manifest entries to a provided
	// // io.Writer. The version of the manifest file is used to determine the
	// // schema to use for writing the entries.
	// WriteEntries(out io.Writer, entries []ManifestEntry) error

	setVersion(int)
}

// manifestFileV1Reader is the Avro reader schema for V1 manifest list entries.
// It handles both spec-correct field names and pre-1.4 Java Iceberg legacy
// names (added_data_files_count etc.) via the alias tags on manifestFileV1,
// and both nullable and non-nullable added_snapshot_id via *int64.
var manifestFileV1Reader = avro.MustSchemaFor[manifestFileV1](
	avro.WithName("manifest_file"),
)

// manifestFileReader is the Avro reader schema for V2+ manifest list entries.
// Alias tags on manifestFile handle pre-1.4 Java Iceberg legacy field names.
var manifestFileReader = avro.MustSchemaFor[manifestFile](
	avro.WithName("manifest_file"),
)

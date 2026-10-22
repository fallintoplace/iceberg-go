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
	"fmt"
)

type ManifestEntryBuilder struct {
	m *manifestEntry
}

func NewManifestEntryBuilder(status ManifestEntryStatus, snapshotID *int64, data DataFile) *ManifestEntryBuilder {
	return &ManifestEntryBuilder{
		m: &manifestEntry{
			EntryStatus: status,
			Snapshot:    snapshotID,
			Data:        data,
		},
	}
}

func (b *ManifestEntryBuilder) SequenceNum(num int64) *ManifestEntryBuilder {
	b.m.SeqNum = &num

	return b
}

func (b *ManifestEntryBuilder) FileSequenceNum(num int64) *ManifestEntryBuilder {
	b.m.FileSeqNum = &num

	return b
}

func (b *ManifestEntryBuilder) Build() ManifestEntry {
	return b.m
}

type manifestEntry struct {
	EntryStatus ManifestEntryStatus `avro:"status"`
	Snapshot    *int64              `avro:"snapshot_id"`
	SeqNum      *int64              `avro:"sequence_number"`
	FileSeqNum  *int64              `avro:"file_sequence_number"`
	Data        DataFile            `avro:"data_file"`
}

func (m *manifestEntry) Status() ManifestEntryStatus { return m.EntryStatus }
func (m *manifestEntry) SnapshotID() int64 {
	if m.Snapshot == nil {
		return -1
	}

	return *m.Snapshot
}

func (m *manifestEntry) SequenceNum() int64 {
	if m.SeqNum == nil {
		return -1
	}

	return *m.SeqNum
}

func (m *manifestEntry) FileSequenceNum() *int64 {
	return m.FileSeqNum
}

func (m *manifestEntry) DataFile() DataFile { return m.Data }

func (m *manifestEntry) inherit(manifest ManifestFile) {
	if m.Snapshot == nil {
		snap := manifest.SnapshotID()
		m.Snapshot = &snap
	}

	manifestSequenceNum := manifest.SequenceNum()
	if manifestSequenceNum != -1 {
		if m.SeqNum == nil && (manifestSequenceNum == initialSequenceNumber || m.EntryStatus == EntryStatusADDED) {
			m.SeqNum = &manifestSequenceNum
		}

		if m.FileSeqNum == nil && (manifestSequenceNum == initialSequenceNumber || m.EntryStatus == EntryStatusADDED) {
			m.FileSeqNum = &manifestSequenceNum
		}
	}

	m.Data.(*dataFile).specID = manifest.PartitionSpecID()
}

func (m *manifestEntry) wrap(status ManifestEntryStatus, newSnapID, newSeq, newFileSeq *int64, data DataFile) ManifestEntry {
	if newSeq != nil && *newSeq == -1 {
		newSeq = nil
	}

	m.EntryStatus = status
	m.Snapshot = newSnapID
	m.SeqNum = newSeq
	m.FileSeqNum = newFileSeq
	m.Data = data

	return m
}

type fallbackManifestEntry struct {
	manifestEntry
	Snapshot int64 `avro:"snapshot_id"`
}

func (f *fallbackManifestEntry) toEntry() *manifestEntry {
	f.manifestEntry.Snapshot = &f.Snapshot

	return &f.manifestEntry
}

func NewManifestEntry(status ManifestEntryStatus, snapshotID *int64, seqNum, fileSeqNum *int64, df DataFile) ManifestEntry {
	return &manifestEntry{
		EntryStatus: status,
		Snapshot:    snapshotID,
		SeqNum:      seqNum,
		FileSeqNum:  fileSeqNum,
		Data:        df,
	}
}

// DataFileBuilder is a helper for building a data file struct which will
// conform to the DataFile interface.
type DataFileBuilder struct {
	d *dataFile
}

// NewDataFileBuilder is passed all of the required fields and then allows
// all of the optional fields to be set by calling the corresponding methods
// before calling [DataFileBuilder.Build] to construct the object.
func NewDataFileBuilder(
	spec PartitionSpec,
	content ManifestEntryContent,
	path string,
	format FileFormat,
	fieldIDToPartitionData map[int]any,
	fieldIDToLogicalType map[int]string,
	fieldIDToFixedSize map[int]int,
	recordCount int64,
	fileSize int64,
) (*DataFileBuilder, error) {
	if content != EntryContentData && content != EntryContentPosDeletes && content != EntryContentEqDeletes {
		return nil, fmt.Errorf(
			"%w: content must be one of %s, %s, or %s",
			ErrInvalidArgument, EntryContentData, EntryContentPosDeletes, EntryContentEqDeletes,
		)
	}

	if path == "" {
		return nil, fmt.Errorf("%w: path cannot be empty", ErrInvalidArgument)
	}

	if format != AvroFile && format != OrcFile && format != ParquetFile && format != PuffinFile {
		return nil, fmt.Errorf(
			"%w: format must be one of %s, %s, %s, or %s",
			ErrInvalidArgument, AvroFile, OrcFile, ParquetFile, PuffinFile,
		)
	}

	if format == PuffinFile && content != EntryContentPosDeletes {
		return nil, fmt.Errorf(
			"%w: %s format is only valid for %s content",
			ErrInvalidArgument, PuffinFile, EntryContentPosDeletes,
		)
	}

	if recordCount <= 0 {
		return nil, fmt.Errorf("%w: record count must be greater than 0", ErrInvalidArgument)
	}

	if fileSize <= 0 {
		return nil, fmt.Errorf("%w: file size must be greater than 0", ErrInvalidArgument)
	}
	partitionData := make(map[string]any)
	fieldNameToID := make(map[string]int)
	for _, p := range spec.fields {
		if pData, ok := fieldIDToPartitionData[p.FieldID]; ok {
			partitionData[p.Name] = pData
			fieldNameToID[p.Name] = p.FieldID
		}
	}

	return &DataFileBuilder{
		d: &dataFile{
			Content:                content,
			Path:                   path,
			Format:                 format,
			PartitionData:          partitionData,
			RecordCount:            recordCount,
			FileSize:               fileSize,
			specID:                 int32(spec.id),
			fieldIDToPartitionData: fieldIDToPartitionData,
			fieldNameToID:          fieldNameToID,
			fieldIDToLogicalType:   fieldIDToLogicalType,
			fieldIDToFixedSize:     fieldIDToFixedSize,
		},
	}, nil
}

// BlockSizeInBytes sets the block size in bytes for the data file. Deprecated in v2.
func (b *DataFileBuilder) BlockSizeInBytes(size int64) *DataFileBuilder {
	b.d.BlockSizeInBytes = size

	return b
}

// ColumnSizes sets the column sizes for the data file.
func (b *DataFileBuilder) ColumnSizes(sizes map[int]int64) *DataFileBuilder {
	b.d.ColSizes = mapToAvroColMap(sizes)

	return b
}

// ValueCounts sets the value counts for the data file.
func (b *DataFileBuilder) ValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.ValCounts = mapToAvroColMap(counts)

	return b
}

// NullValueCounts sets the null value counts for the data file.
func (b *DataFileBuilder) NullValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.NullCounts = mapToAvroColMap(counts)

	return b
}

// NaNValueCounts sets the NaN value counts for the data file.
func (b *DataFileBuilder) NaNValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.NaNCounts = mapToAvroColMap(counts)

	return b
}

// DistinctValueCounts sets the distinct value counts for the data file.
//
// Deprecated: distinct_counts (field 111) is deprecated in every
// version of the Iceberg spec (apache/iceberg#12182). The Avro
// manifest-entry schemas omit the field for v1, v2, and v3, so values
// set here are not transported in manifests written by this library.
// The setter is retained for round-tripping legacy DataFiles read from
// older manifests; new code should not call it.
func (b *DataFileBuilder) DistinctValueCounts(counts map[int]int64) *DataFileBuilder {
	b.d.DistinctCounts = mapToAvroColMap(counts)

	return b
}

// LowerBoundValues sets the lower bound values for the data file.
func (b *DataFileBuilder) LowerBoundValues(bounds map[int][]byte) *DataFileBuilder {
	b.d.LowerBounds = mapToAvroColMap(bounds)

	return b
}

// UpperBoundValues sets the upper bound values for the data file.
func (b *DataFileBuilder) UpperBoundValues(bounds map[int][]byte) *DataFileBuilder {
	b.d.UpperBounds = mapToAvroColMap(bounds)

	return b
}

// KeyMetadata sets the key metadata for the data file.
func (b *DataFileBuilder) KeyMetadata(key []byte) *DataFileBuilder {
	b.d.Key = &key

	return b
}

// SplitOffsets sets the split offsets for the data file.
func (b *DataFileBuilder) SplitOffsets(offsets []int64) *DataFileBuilder {
	b.d.Splits = &offsets

	return b
}

// EqualityFieldIDs sets the equality field ids for the data file.
func (b *DataFileBuilder) EqualityFieldIDs(ids []int) *DataFileBuilder {
	b.d.EqualityIDs = &ids

	return b
}

// SortOrderID sets the sort order id for the data file.
func (b *DataFileBuilder) SortOrderID(id int) *DataFileBuilder {
	b.d.SortOrder = &id

	return b
}

func (b *DataFileBuilder) FirstRowID(id int64) *DataFileBuilder {
	b.d.FirstRowIDField = &id

	return b
}

func (b *DataFileBuilder) ReferencedDataFile(path string) *DataFileBuilder {
	b.d.ReferencedDataFileField = &path

	return b
}

func (b *DataFileBuilder) ContentOffset(offset int64) *DataFileBuilder {
	b.d.ContentOffsetField = &offset

	return b
}

func (b *DataFileBuilder) ContentSizeInBytes(size int64) *DataFileBuilder {
	b.d.ContentSizeInBytesField = &size

	return b
}

func (b *DataFileBuilder) Build() DataFile {
	return b.d
}

// DataFile is the interface for reading the information about a
// given data file indicated by an entry in a manifest list.
type DataFile interface {
	// ContentType is the type of the content stored by the data file,
	// either Data, Equality deletes, or Position deletes. All v1 files
	// are Data files.
	ContentType() ManifestEntryContent
	// FilePath is the full URI for the file, complete with FS scheme.
	FilePath() string
	// FileFormat is the format of the data file, AVRO, Orc, or Parquet.
	FileFormat() FileFormat
	// Partition returns a mapping of field id to partition value for
	// each of the partition spec's fields.
	Partition() map[int]any
	// Count returns the number of records in this file.
	Count() int64
	// FileSizeBytes is the total file size in bytes.
	FileSizeBytes() int64
	// ColumnSizes is a mapping from column id to the total size on disk
	// of all regions that store the column. Does not include bytes
	// necessary to read other columns, like footers. Map will be nil for
	// row-oriented formats (avro).
	ColumnSizes() map[int]int64
	// ValueCounts is a mapping from column id to the number of values
	// in the column, including null and NaN values.
	ValueCounts() map[int]int64
	// NullValueCounts is a mapping from column id to the number of
	// null values in the column.
	NullValueCounts() map[int]int64
	// NaNValueCounts is a mapping from column id to the number of NaN
	// values in the column.
	NaNValueCounts() map[int]int64
	// DistictValueCounts is a mapping from column id to the number of
	// distinct values in the column. Distinct counts must be derived
	// using values in the file by counting or using sketches, but not
	// using methods like merging existing distinct counts.
	DistinctValueCounts() map[int]int64
	// LowerBoundValues is a mapping from column id to the lower bounded
	// value of the column, serialized as binary. Each value in the column
	// must be less than or requal to all non-null, non-NaN values in the
	// column for the file.
	LowerBoundValues() map[int][]byte
	// UpperBoundValues is a mapping from column id to the upper bounded
	// value of the column, serialized as binary. Each value in the column
	// must be greater than or equal to all non-null, non-NaN values in
	// the column for the file.
	UpperBoundValues() map[int][]byte
	// KeyMetadata is implementation-specific key metadata for encryption.
	KeyMetadata() []byte
	// SplitOffsets are the split offsets for the data file. For example,
	// all row group offsets in a Parquet file. Must be sorted ascending.
	SplitOffsets() []int64
	// EqualityFieldIDs are used to determine row equality in equality
	// delete files. It is required when the content type is
	// EntryContentEqDeletes.
	EqualityFieldIDs() []int
	// SortOrderID returns the id representing the sort order for this
	// file, or nil if there is no sort order.
	SortOrderID() *int
	// SpecID returns the partition spec id for this data file, inherited
	// from the manifest that the data file was read from
	SpecID() int32
	// FirstRowID returns the first row ID for this data file ( v3+ only )
	FirstRowID() *int64
	// ReferencedDataFile returns the location of the data file that deletion vector reference
	ReferencedDataFile() *string
	// ContentOffset returns the offset in the file where the content starts ( v3+ only )
	ContentOffset() *int64
	// ContentSizeInBytes returns the length of referenced contented stored in the file (v3+ only)
	ContentSizeInBytes() *int64
}

// ManifestEntry is an interface for both v1 and v2 manifest entries.
type ManifestEntry interface {
	// Status returns the type of the file tracked by this entry.
	// Deletes are informational only and not used in scans.
	Status() ManifestEntryStatus
	// SnapshotID is the id where the file was added, or deleted,
	// if null it is inherited from the manifest list.
	SnapshotID() int64
	// SequenceNum returns the data sequence number of the file.
	// If it was null and the status is EntryStatusADDED then it
	// is inherited from the manifest list.
	SequenceNum() int64
	// FileSequenceNum returns the file sequence number indicating
	// when the file was added. If it was null and the status is
	// EntryStatusADDED then it is inherited from the manifest list.
	FileSequenceNum() *int64
	// DataFile provides the information about the data file indicated
	// by this manifest entry.
	DataFile() DataFile

	inherit(manifest ManifestFile)

	wrap(status ManifestEntryStatus, snapshotID, seqNum, fileSeqNum *int64, datafile DataFile) ManifestEntry
}

var PositionalDeleteSchema = NewSchema(0,
	NestedField{ID: 2147483546, Type: PrimitiveTypes.String, Name: "file_path", Required: true},
	NestedField{ID: 2147483545, Type: PrimitiveTypes.Int64, Name: "pos", Required: true},
)

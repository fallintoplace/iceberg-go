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
	"maps"
	"slices"
	"strings"
	"sync"
)

// ManifestEntryContent defines constants for the type of file contents
// in the file entries. Data, Position based deletes and equality based
// deletes.
type ManifestEntryContent int8

const (
	EntryContentData       ManifestEntryContent = 0
	EntryContentPosDeletes ManifestEntryContent = 1
	EntryContentEqDeletes  ManifestEntryContent = 2
)

func (m ManifestEntryContent) String() string {
	switch m {
	case EntryContentData:
		return "Data"
	case EntryContentPosDeletes:
		return "Positional_Deletes"
	case EntryContentEqDeletes:
		return "Equality_Deletes"
	default:
		return "UNKNOWN"
	}
}

// FileFormat defines constants for the format of data files.
type FileFormat string

const (
	AvroFile    FileFormat = "AVRO"
	OrcFile     FileFormat = "ORC"
	ParquetFile FileFormat = "PARQUET"
	PuffinFile  FileFormat = "PUFFIN"
)

// FileFormatFromString parses a file format string (case-insensitive).
func FileFormatFromString(s string) (FileFormat, error) {
	switch strings.ToUpper(s) {
	case string(ParquetFile):
		return ParquetFile, nil
	case string(OrcFile):
		return OrcFile, nil
	case string(AvroFile):
		return AvroFile, nil
	case string(PuffinFile):
		return PuffinFile, nil
	default:
		return "", fmt.Errorf("unknown file format: %s", s)
	}
}


type dataFile struct {
	Content          ManifestEntryContent `avro:"content"`
	projected        bool
	Path             string                `avro:"file_path"`
	Format           FileFormat            `avro:"file_format"`
	PartitionData    map[string]any        `avro:"partition"`
	RecordCount      int64                 `avro:"record_count"`
	FileSize         int64                 `avro:"file_size_in_bytes"`
	BlockSizeInBytes int64                 `avro:"block_size_in_bytes"`
	ColSizes         *[]colMap[int, int64] `avro:"column_sizes"`
	ValCounts        *[]colMap[int, int64] `avro:"value_counts"`
	NullCounts       *[]colMap[int, int64] `avro:"null_value_counts"`
	NaNCounts        *[]colMap[int, int64] `avro:"nan_value_counts"`
	// DistinctCounts holds distinct_counts (field 111), deprecated in the
	// spec. It has no accessor and writers never encode it; it stays so
	// legacy manifests that carry the field still decode.
	DistinctCounts          *[]colMap[int, int64]  `avro:"distinct_counts"`
	LowerBounds             *[]colMap[int, []byte] `avro:"lower_bounds"`
	UpperBounds             *[]colMap[int, []byte] `avro:"upper_bounds"`
	Key                     *[]byte                `avro:"key_metadata"`
	Splits                  *[]int64               `avro:"split_offsets"`
	EqualityIDs             *[]int                 `avro:"equality_ids"`
	SortOrder               *int                   `avro:"sort_order_id"`
	FirstRowIDField         *int64                 `avro:"first_row_id"`
	ReferencedDataFileField *string                `avro:"referenced_data_file"`
	ContentOffsetField      *int64                 `avro:"content_offset"`
	ContentSizeInBytesField *int64                 `avro:"content_size_in_bytes"`

	colSizeMap    map[int]int64
	valCntMap     map[int]int64
	nullCntMap    map[int]int64
	nanCntMap     map[int]int64
	lowerBoundMap map[int][]byte
	upperBoundMap map[int][]byte

	// used for partition retrieval
	fieldNameToID          map[string]int
	fieldIDToLogicalType   map[int]string
	fieldIDToPartitionData map[int]any
	fieldIDToDecimalScale  map[int]int

	specID          int32
	initPartition   sync.Once
	initColumnStats sync.Once
}


func (d *dataFile) ContentType() ManifestEntryContent { return d.Content }
func (d *dataFile) FilePath() string                  { return d.Path }
func (d *dataFile) FileFormat() FileFormat            { return d.Format }

// Partition returns the partition data as a map of partition field ID to value.
func (d *dataFile) Partition() map[int]any {
	d.initPartitionData()

	return clonePartitionMap(d.fieldIDToPartitionData)
}

func clonePartitionMap(src map[int]any) map[int]any {
	if src == nil {
		return nil
	}

	// Manifest decoding produces immutable scalar/literal values and []byte
	// for binary and fixed values. Clone the latter explicitly; arbitrary
	// caller-defined mutable values are not recursively copied.
	out := maps.Clone(src)
	for id, value := range out {
		if bytes, ok := value.([]byte); ok {
			out[id] = slices.Clone(bytes)
		}
	}

	return out
}

func cloneByteMap(src map[int][]byte) map[int][]byte {
	if src == nil {
		return nil
	}

	out := make(map[int][]byte, len(src))
	for id, value := range src {
		out[id] = slices.Clone(value)
	}

	return out
}

func mapToAvroColMapClonedBytes(m map[int][]byte) *[]colMap[int, []byte] {
	if m == nil {
		return nil
	}

	out := make([]colMap[int, []byte], 0, len(m))
	for k, v := range m {
		out = append(out, colMap[int, []byte]{Key: k, Value: slices.Clone(v)})
	}

	return &out
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}

	cloned := *value

	return &cloned
}

func (d *dataFile) Count() int64         { return d.RecordCount }
func (d *dataFile) FileSizeBytes() int64 { return d.FileSize }
func (d *dataFile) SpecID() int32        { return d.specID }

func (d *dataFile) ColumnSizes() map[int]int64 {
	d.initColumnStatsData()

	return maps.Clone(d.colSizeMap)
}

func (d *dataFile) ValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return maps.Clone(d.valCntMap)
}

func (d *dataFile) NullValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return maps.Clone(d.nullCntMap)
}

func (d *dataFile) NaNValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return maps.Clone(d.nanCntMap)
}

func (d *dataFile) LowerBoundValues() map[int][]byte {
	d.initColumnStatsData()

	return cloneByteMap(d.lowerBoundMap)
}

func (d *dataFile) UpperBoundValues() map[int][]byte {
	d.initColumnStatsData()

	return cloneByteMap(d.upperBoundMap)
}

func (d *dataFile) KeyMetadata() []byte {
	if d.Key == nil {
		return nil
	}

	return slices.Clone(*d.Key)
}

func (d *dataFile) SplitOffsets() []int64 {
	if d.Splits == nil {
		return nil
	}

	return slices.Clone(*d.Splits)
}

func (d *dataFile) EqualityFieldIDs() []int {
	if d.EqualityIDs == nil {
		return nil
	}

	return slices.Clone(*d.EqualityIDs)
}

func (d *dataFile) SortOrderID() *int { return clonePointer(d.SortOrder) }

func (d *dataFile) FirstRowID() *int64          { return clonePointer(d.FirstRowIDField) }
func (d *dataFile) ReferencedDataFile() *string { return clonePointer(d.ReferencedDataFileField) }
func (d *dataFile) ContentSizeInBytes() *int64  { return clonePointer(d.ContentSizeInBytesField) }
func (d *dataFile) ContentOffset() *int64       { return clonePointer(d.ContentOffsetField) }


// DataFileBuilder is a helper for building a data file struct which will
// conform to the DataFile interface.
type DataFileBuilder struct {
	d *dataFile
}

// NewDataFileBuilder is passed all of the required fields and then allows
// all of the optional fields to be set by calling the corresponding methods
// before calling [DataFileBuilder.Build] to construct the object.
// The fieldIDToFixedSize argument is retained for source compatibility and is
// ignored; manifest schemas determine decimal fixed widths during encoding.
// Byte-slice partition values are cloned. Other partition values must be the
// immutable scalar or literal values produced by Iceberg's manifest decoder;
// mutable caller-defined values are retained by reference.
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
	_ = fieldIDToFixedSize
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

	if recordCount < 0 {
		return nil, fmt.Errorf("%w: record count must be non-negative", ErrInvalidArgument)
	}

	if fileSize < 0 {
		return nil, fmt.Errorf("%w: file size must be non-negative", ErrInvalidArgument)
	}
	partitionData := make(map[string]any)
	fieldNameToID := make(map[string]int)
	for _, p := range spec.fields {
		if pData, ok := fieldIDToPartitionData[p.FieldID]; ok {
			if bytes, ok := pData.([]byte); ok {
				pData = slices.Clone(bytes)
			}
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
			fieldIDToPartitionData: clonePartitionMap(fieldIDToPartitionData),
			fieldNameToID:          fieldNameToID,
			fieldIDToLogicalType:   maps.Clone(fieldIDToLogicalType),
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

// LowerBoundValues sets the lower bound values for the data file.
func (b *DataFileBuilder) LowerBoundValues(bounds map[int][]byte) *DataFileBuilder {
	b.d.LowerBounds = mapToAvroColMapClonedBytes(bounds)

	return b
}

// UpperBoundValues sets the upper bound values for the data file.
func (b *DataFileBuilder) UpperBoundValues(bounds map[int][]byte) *DataFileBuilder {
	b.d.UpperBounds = mapToAvroColMapClonedBytes(bounds)

	return b
}

// KeyMetadata sets the key metadata for the data file.
func (b *DataFileBuilder) KeyMetadata(key []byte) *DataFileBuilder {
	cloned := slices.Clone(key)
	b.d.Key = &cloned

	return b
}

// SplitOffsets sets the split offsets for the data file.
func (b *DataFileBuilder) SplitOffsets(offsets []int64) *DataFileBuilder {
	cloned := slices.Clone(offsets)
	b.d.Splits = &cloned

	return b
}

// EqualityFieldIDs sets the equality field ids for the data file.
func (b *DataFileBuilder) EqualityFieldIDs(ids []int) *DataFileBuilder {
	cloned := slices.Clone(ids)
	b.d.EqualityIDs = &cloned

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


var PositionalDeleteSchema = NewSchema(0,
	NestedField{ID: DeleteFilePathFieldID, Type: PrimitiveTypes.String, Name: "file_path", Required: true},
	NestedField{ID: DeleteFilePosFieldID, Type: PrimitiveTypes.Int64, Name: "pos", Required: true},
)

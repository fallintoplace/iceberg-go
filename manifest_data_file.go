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
	"math/big"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/iceberg-go/internal"
	"github.com/google/uuid"
	"github.com/twmb/avro/atype"
)

// ManifestEntryStatus defines constants for the entry status of
// existing, added or deleted.
type ManifestEntryStatus int8

const (
	EntryStatusEXISTING ManifestEntryStatus = 0
	EntryStatusADDED    ManifestEntryStatus = 1
	EntryStatusDELETED  ManifestEntryStatus = 2
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

type colMap[K, V any] struct {
	Key   K `avro:"key"`
	Value V `avro:"value"`
}

func avroColMapToMap[K comparable, V any](c *[]colMap[K, V]) map[K]V {
	if c == nil {
		return nil
	}

	out := make(map[K]V, len(*c))
	for _, data := range *c {
		out[data.Key] = data.Value
	}

	return out
}

func mapToAvroColMap[K comparable, V any](m map[K]V) *[]colMap[K, V] {
	if m == nil {
		return nil
	}

	out := make([]colMap[K, V], 0, len(m))
	for k, v := range m {
		out = append(out, colMap[K, V]{Key: k, Value: v})
	}

	return &out
}

func avroPartitionData(input map[int]any, logicalTypes map[int]string) map[int]any {
	out := make(map[int]any)
	for k, v := range input {
		if logical, ok := logicalTypes[k]; ok {
			out[k] = convertLogicalTypeValue(v, logical)
		} else {
			out[k] = v
		}
	}

	return out
}

func convertLogicalTypeValue(v any, logicalType string) any {
	switch logicalType {
	case atype.Date:
		return convertDateValue(v)
	case atype.TimeMicros:
		return convertTimeMicrosValue(v)
	case atype.TimestampMicros:
		return convertTimestampMicrosValue(v)
	case atype.Decimal:
		return convertDecimalValue(v)
	case atype.UUID:
		return convertUUIDValue(v)
	default:
		return v
	}
}

func convertDateValue(v any) any {
	if d, ok := v.(Date); ok {
		return int32(d)
	}

	return v
}

func convertTimeMicrosValue(v any) any {
	if t, ok := v.(Time); ok {
		return int64(t)
	}

	return v
}

func convertTimestampMicrosValue(v any) any {
	if ts, ok := v.(Timestamp); ok {
		return int64(ts)
	}

	return v
}

func convertDecimalValue(v any) any {
	if dec, ok := v.(Decimal); ok {
		fixedSize := internal.DecimalRequiredBytes(len(dec.String()))
		bytes, err := DecimalLiteral(dec).MarshalBinary()
		if err != nil {
			return v
		}

		return padOrTruncateBytes(bytes, fixedSize)
	}

	return v
}

func convertUUIDValue(v any) any {
	if uuidVal, ok := v.(uuid.UUID); ok {
		return [16]byte(uuidVal)
	}

	return v
}

func padOrTruncateBytes(bytes []byte, size int) []byte {
	if len(bytes) >= size {
		return bytes[len(bytes)-size:]
	}
	padded := slices.Grow(bytes, size-len(bytes))

	return append(make([]byte, size-len(bytes)), padded...)
}

type dataFile struct {
	Content                 ManifestEntryContent   `avro:"content"`
	Path                    string                 `avro:"file_path"`
	Format                  FileFormat             `avro:"file_format"`
	PartitionData           map[string]any         `avro:"partition"`
	RecordCount             int64                  `avro:"record_count"`
	FileSize                int64                  `avro:"file_size_in_bytes"`
	BlockSizeInBytes        int64                  `avro:"block_size_in_bytes"`
	ColSizes                *[]colMap[int, int64]  `avro:"column_sizes"`
	ValCounts               *[]colMap[int, int64]  `avro:"value_counts"`
	NullCounts              *[]colMap[int, int64]  `avro:"null_value_counts"`
	NaNCounts               *[]colMap[int, int64]  `avro:"nan_value_counts"`
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

	colSizeMap     map[int]int64
	valCntMap      map[int]int64
	nullCntMap     map[int]int64
	nanCntMap      map[int]int64
	distinctCntMap map[int]int64
	lowerBoundMap  map[int][]byte
	upperBoundMap  map[int][]byte

	// used for partition retrieval
	fieldNameToID          map[string]int
	fieldIDToLogicalType   map[int]string
	fieldIDToPartitionData map[int]any
	fieldIDToFixedSize     map[int]int

	specID          int32
	initPartition   sync.Once
	initColumnStats sync.Once
}

func (d *dataFile) initPartitionData() {
	d.initPartition.Do(func() {
		if len(d.fieldIDToPartitionData) < len(d.PartitionData) {
			d.fieldIDToPartitionData = make(map[int]any, len(d.PartitionData))
			for k, v := range d.PartitionData {
				if id, ok := d.fieldNameToID[k]; ok {
					convertedValue := d.convertAvroValueToIcebergType(v, id)
					d.fieldIDToPartitionData[id] = convertedValue
				}
			}
		}
	})
}

func (d *dataFile) initColumnStatsData() {
	d.initColumnStats.Do(func() {
		d.colSizeMap = avroColMapToMap(d.ColSizes)
		d.valCntMap = avroColMapToMap(d.ValCounts)
		d.nullCntMap = avroColMapToMap(d.NullCounts)
		d.nanCntMap = avroColMapToMap(d.NaNCounts)
		d.distinctCntMap = avroColMapToMap(d.DistinctCounts)
		d.lowerBoundMap = avroColMapToMap(d.LowerBounds)
		d.upperBoundMap = avroColMapToMap(d.UpperBounds)
	})
}

func (d *dataFile) convertAvroValueToIcebergType(v any, fieldID int) any {
	if v == nil {
		return nil
	}

	if logicalType, ok := d.fieldIDToLogicalType[fieldID]; ok {
		// twmb/avro returns rich Go types (time.Time, time.Duration,
		// *big.Rat, [16]byte) when the file schema includes a logicalType,
		// but raw primitives (int32, int64, []byte, string) when it does
		// not. Each case handles both possibilities.
		switch logicalType {
		case atype.Date:
			if val, ok := v.(time.Time); ok {
				return Date(val.Truncate(24*time.Hour).Unix() / int64((time.Hour * 24).Seconds()))
			}
			if val, ok := v.(int32); ok {
				return Date(val)
			}

			// Unreachable with twmb/avro: an int+date logical type decodes
			// to time.Time and a plain int to int32, so v is always one of
			// the two cases above. Returning v rather than panicking keeps a
			// future decoder change from crashing here, but callers that type
			// assert iceberg.Date would then fail; do not add a guard that
			// silently coerces other types, which reintroduces #1200.
			return v
		case atype.TimeMillis:
			if val, ok := v.(time.Duration); ok {
				return Time(val.Milliseconds())
			}

			return Time(v.(int64))
		case atype.TimeMicros:
			if val, ok := v.(time.Duration); ok {
				return Time(val.Microseconds())
			}

			return Time(v.(int64))
		case atype.TimestampMillis:
			if val, ok := v.(time.Time); ok {
				return Timestamp(val.UTC().UnixMilli())
			}

			return Timestamp(v.(int64))
		case atype.TimestampMicros:
			if val, ok := v.(time.Time); ok {
				return Timestamp(val.UTC().UnixMicro())
			}

			return Timestamp(v.(int64))
		case atype.TimestampNanos:
			if val, ok := v.(time.Time); ok {
				return TimestampNano(val.UTC().UnixNano())
			}

			return TimestampNano(v.(int64))
		case atype.Decimal:
			if r, ok := v.(*big.Rat); ok {
				scale := d.fieldIDToFixedSize[fieldID]
				scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
				unscaled := new(big.Int).Mul(r.Num(), scaleFactor)
				unscaled = unscaled.Div(unscaled, r.Denom())

				return DecimalLiteral{
					Scale: scale,
					Val:   decimal128.FromBigInt(unscaled),
				}
			}

			return v
		case atype.UUID:
			if uuidArr, ok := v.([16]byte); ok {
				return uuid.UUID(uuidArr)
			}

			return v
		}
	}

	return v
}

func (d *dataFile) setFieldNameToIDMap(m map[string]int) { d.fieldNameToID = m }
func (d *dataFile) setFieldIDToLogicalTypeMap(m map[int]string) {
	d.fieldIDToLogicalType = m
}
func (d *dataFile) setFieldIDToFixedSizeMap(m map[int]int) { d.fieldIDToFixedSize = m }

func (d *dataFile) ContentType() ManifestEntryContent { return d.Content }
func (d *dataFile) FilePath() string                  { return d.Path }
func (d *dataFile) FileFormat() FileFormat            { return d.Format }

// Partition returns the partition data as a map of partition field ID to value.
func (d *dataFile) Partition() map[int]any {
	d.initPartitionData()

	return d.fieldIDToPartitionData
}

func (d *dataFile) Count() int64         { return d.RecordCount }
func (d *dataFile) FileSizeBytes() int64 { return d.FileSize }
func (d *dataFile) SpecID() int32        { return d.specID }

func (d *dataFile) ColumnSizes() map[int]int64 {
	d.initColumnStatsData()

	return d.colSizeMap
}

func (d *dataFile) ValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return d.valCntMap
}

func (d *dataFile) NullValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return d.nullCntMap
}

func (d *dataFile) NaNValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return d.nanCntMap
}

func (d *dataFile) DistinctValueCounts() map[int]int64 {
	d.initColumnStatsData()

	return d.distinctCntMap
}

func (d *dataFile) LowerBoundValues() map[int][]byte {
	d.initColumnStatsData()

	return d.lowerBoundMap
}

func (d *dataFile) UpperBoundValues() map[int][]byte {
	d.initColumnStatsData()

	return d.upperBoundMap
}

func (d *dataFile) KeyMetadata() []byte {
	if d.Key == nil {
		return nil
	}

	return *d.Key
}

func (d *dataFile) SplitOffsets() []int64 {
	if d.Splits == nil {
		return nil
	}

	return *d.Splits
}

func (d *dataFile) EqualityFieldIDs() []int {
	if d.EqualityIDs == nil {
		return nil
	}

	return *d.EqualityIDs
}

func (d *dataFile) SortOrderID() *int { return d.SortOrder }

func (d *dataFile) FirstRowID() *int64          { return d.FirstRowIDField }
func (d *dataFile) ReferencedDataFile() *string { return d.ReferencedDataFileField }
func (d *dataFile) ContentSizeInBytes() *int64  { return d.ContentSizeInBytesField }
func (d *dataFile) ContentOffset() *int64       { return d.ContentOffsetField }

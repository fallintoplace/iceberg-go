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
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/google/uuid"
	"github.com/twmb/avro/atype"
)

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

func convertLogicalTypeValue(v any, logicalType string, fixedSize int) (any, error) {
	switch logicalType {
	case atype.Date:
		return convertDateValue(v), nil
	case atype.TimeMicros:
		return convertTimeMicrosValue(v), nil
	case atype.TimestampMicros:
		return convertTimestampMicrosValue(v), nil
	case atype.Decimal:
		return convertDecimalValue(v, fixedSize)
	case atype.UUID:
		return convertUUIDValue(v), nil
	default:
		return v, nil
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

func convertDecimalValue(v any, fixedSize int) (any, error) {
	switch dec := v.(type) {
	case Decimal:
		return encodeDecimalBytes(DecimalLiteral(dec), fixedSize)
	case DecimalLiteral:
		return encodeDecimalBytes(dec, fixedSize)
	default:
		return v, nil
	}
}

func encodeDecimalBytes(dec DecimalLiteral, fixedSize int) ([]byte, error) {
	if fixedSize <= 0 {
		return nil, errors.New("missing decimal fixed size")
	}

	bytes, err := dec.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return fitDecimalBytes(bytes, fixedSize)
}

func convertUUIDValue(v any) any {
	if uuidVal, ok := v.(uuid.UUID); ok {
		return [16]byte(uuidVal)
	}

	return v
}

func fitDecimalBytes(bytes []byte, size int) ([]byte, error) {
	if size <= 0 {
		return nil, fmt.Errorf("decimal fixed size must be positive: %d", size)
	}

	if len(bytes) == size {
		return bytes, nil
	}

	if len(bytes) < size {
		signByte := byte(0x00)
		if bytes[0]&0x80 != 0 {
			signByte = 0xff
		}
		padded := make([]byte, size)
		for i := range padded[:size-len(bytes)] {
			padded[i] = signByte
		}
		copy(padded[size-len(bytes):], bytes)

		return padded, nil
	}

	retained := bytes[len(bytes)-size:]
	signByte := byte(0x00)
	if retained[0]&0x80 != 0 {
		signByte = 0xff
	}
	for _, b := range bytes[:len(bytes)-size] {
		if b != signByte {
			return nil, fmt.Errorf("decimal value of %d bytes does not fit fixed size %d", len(bytes), size)
		}
	}

	return retained, nil
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
			// iceberg.Time counts microseconds; a time-millis Duration must be
			// read as microseconds (val.Milliseconds() undercounts 1000x). The
			// raw arm converts the millisecond wire value to microseconds too.
			if val, ok := v.(time.Duration); ok {
				return Time(val.Microseconds())
			}
			if ms, ok := avroIntAsInt64(v); ok {
				return Time(ms * 1000)
			}

			return v
		case atype.TimeMicros:
			if val, ok := v.(time.Duration); ok {
				return Time(val.Microseconds())
			}
			if us, ok := avroIntAsInt64(v); ok {
				return Time(us)
			}

			return v
		case atype.TimestampMillis:
			// iceberg.Timestamp counts microseconds; UnixMilli() undercounts
			// 1000x. Read both the time.Time and the raw millisecond wire value
			// as microseconds.
			if val, ok := v.(time.Time); ok {
				return Timestamp(val.UTC().UnixMicro())
			}
			if ms, ok := avroIntAsInt64(v); ok {
				return Timestamp(ms * 1000)
			}

			return v
		case atype.TimestampMicros:
			if val, ok := v.(time.Time); ok {
				return Timestamp(val.UTC().UnixMicro())
			}
			if us, ok := avroIntAsInt64(v); ok {
				return Timestamp(us)
			}

			return v
		case atype.TimestampNanos:
			if val, ok := v.(time.Time); ok {
				return TimestampNano(val.UTC().UnixNano())
			}
			if ns, ok := avroIntAsInt64(v); ok {
				return TimestampNano(ns)
			}

			return v
		case atype.Decimal:
			if r, ok := v.(*big.Rat); ok {
				scale := d.fieldIDToDecimalScale[fieldID]
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

// avroIntAsInt64 extracts an Avro int (int32) or long (int64) partition value
// as an int64. A manifest from a foreign writer may declare a time/timestamp
// logicalType on an underlying Avro type the spec does not permit for it (e.g.
// timestamp-millis on an int). twmb/avro drops such an invalid logicalType when
// decoding and yields the raw primitive, yet Schema.Root still reports the
// logicalType, so the conversion arms must accept the raw integer instead of an
// unchecked assertion that would panic (apache/iceberg-go#1847).
func avroIntAsInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	}

	return 0, false
}

func (d *dataFile) setFieldNameToIDMap(m map[string]int) { d.fieldNameToID = m }
func (d *dataFile) setFieldIDToLogicalTypeMap(m map[int]string) {
	d.fieldIDToLogicalType = m
}

func (d *dataFile) setFieldIDToDecimalScaleMap(m map[int]int) {
	d.fieldIDToDecimalScale = m
}

// normalizeFormat sets d.Format to the FileFormat constant matching its
// decoded spelling, or returns an error if it names no known format.
// Every Avro decode path must call it before the value is compared or
// exposed.
func (d *dataFile) normalizeFormat() error {
	format, err := FileFormatFromString(string(d.Format))
	if err != nil {
		return fmt.Errorf("data file %q: %w", d.FilePath(), err)
	}
	d.Format = format

	return nil
}

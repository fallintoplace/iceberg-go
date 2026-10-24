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
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
)

type fieldStats interface {
	toSummary() FieldSummary
	update(value any) error
}

type partitionFieldStats[T LiteralType] struct {
	containsNull bool
	containsNan  bool
	min          *T
	max          *T

	cmp     Comparator[T]
	convert partitionValueConverter[T]
}

func clonePartitionStatValue[T LiteralType](value T) T {
	if data, ok := any(value).([]byte); ok {
		return any(slices.Clone(data)).(T)
	}

	return value
}

type partitionValueConverter[T LiteralType] func(any) (T, bool)

func partitionLiteralValue(value any) (any, bool) {
	switch value.(type) {
	case AboveMaxLiteral, BelowMinLiteral:
		return nil, false
	}

	literal, ok := value.(Literal)
	if !ok {
		return nil, false
	}

	return literal.Any(), true
}

// unknownPartitionFieldStats records null presence but omits bounds because
// the dropped source type needed to encode and compare non-null values is lost.
type unknownPartitionFieldStats struct {
	containsNull bool
}

func (p *unknownPartitionFieldStats) toSummary() FieldSummary {
	containsNaN := false

	return FieldSummary{ContainsNull: p.containsNull, ContainsNaN: &containsNaN}
}

func (p *unknownPartitionFieldStats) update(any) error {
	// Dropped-source values are normalized to Avro null when the entry is
	// encoded, even if an in-memory data file still carries a historical value.
	p.containsNull = true

	return nil
}

func makePartitionFieldStats[T LiteralType](convert partitionValueConverter[T]) fieldStats {
	return &partitionFieldStats[T]{
		cmp:     getComparator[T](),
		convert: convert,
	}
}

func convertPartitionBool(value any) (bool, bool) {
	switch value := value.(type) {
	case bool:
		return value, true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionBool(literal)
		}

		return false, false
	}
}

func convertPartitionInt32(value any) (int32, bool) {
	switch value := value.(type) {
	case int:
		return int32(value), true
	case int8:
		return int32(value), true
	case int16:
		return int32(value), true
	case int32:
		return value, true
	case int64:
		return int32(value), true
	case uint:
		return int32(value), true
	case uint8:
		return int32(value), true
	case uint16:
		return int32(value), true
	case uint32:
		return int32(value), true
	case uint64:
		return int32(value), true
	case uintptr:
		return int32(value), true
	case float32:
		return int32(value), true
	case float64:
		return int32(value), true
	case Date:
		return int32(value), true
	case Time:
		return int32(value), true
	case Timestamp:
		return int32(value), true
	case TimestampNano:
		return int32(value), true
	case time.Duration:
		return int32(value), true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionInt32(literal)
		}

		return 0, false
	}
}

func convertPartitionInt64(value any) (int64, bool) {
	switch value := value.(type) {
	case int:
		return int64(value), true
	case int8:
		return int64(value), true
	case int16:
		return int64(value), true
	case int32:
		return int64(value), true
	case int64:
		return value, true
	case uint:
		return int64(value), true
	case uint8:
		return int64(value), true
	case uint16:
		return int64(value), true
	case uint32:
		return int64(value), true
	case uint64:
		return int64(value), true
	case uintptr:
		return int64(value), true
	case float32:
		return int64(value), true
	case float64:
		return int64(value), true
	case Date:
		return int64(value), true
	case Time:
		return int64(value), true
	case Timestamp:
		return int64(value), true
	case TimestampNano:
		return int64(value), true
	case time.Duration:
		return int64(value), true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionInt64(literal)
		}

		return 0, false
	}
}

func convertPartitionFloat32(value any) (float32, bool) {
	switch value := value.(type) {
	case int:
		return float32(value), true
	case int8:
		return float32(value), true
	case int16:
		return float32(value), true
	case int32:
		return float32(value), true
	case int64:
		return float32(value), true
	case uint:
		return float32(value), true
	case uint8:
		return float32(value), true
	case uint16:
		return float32(value), true
	case uint32:
		return float32(value), true
	case uint64:
		return float32(value), true
	case uintptr:
		return float32(value), true
	case float32:
		return value, true
	case float64:
		return float32(value), true
	case Date:
		return float32(value), true
	case Time:
		return float32(value), true
	case Timestamp:
		return float32(value), true
	case TimestampNano:
		return float32(value), true
	case time.Duration:
		return float32(value), true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionFloat32(literal)
		}

		return 0, false
	}
}

func convertPartitionFloat64(value any) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int8:
		return float64(value), true
	case int16:
		return float64(value), true
	case int32:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint:
		return float64(value), true
	case uint8:
		return float64(value), true
	case uint16:
		return float64(value), true
	case uint32:
		return float64(value), true
	case uint64:
		return float64(value), true
	case uintptr:
		return float64(value), true
	case float32:
		return float64(value), true
	case float64:
		return value, true
	case Date:
		return float64(value), true
	case Time:
		return float64(value), true
	case Timestamp:
		return float64(value), true
	case TimestampNano:
		return float64(value), true
	case time.Duration:
		return float64(value), true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionFloat64(literal)
		}

		return 0, false
	}
}

func convertPartitionString(value any) (string, bool) {
	switch value := value.(type) {
	case string:
		return value, true
	case []byte:
		return string(value), true
	case []rune:
		return string(value), true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionString(literal)
		}

		return "", false
	}
}

func convertPartitionBytes(value any) ([]byte, bool) {
	switch value := value.(type) {
	case []byte:
		return value, true
	case string:
		return []byte(value), true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionBytes(literal)
		}

		return nil, false
	}
}

func convertPartitionUUID(value any) (uuid.UUID, bool) {
	switch value := value.(type) {
	case uuid.UUID:
		return value, true
	case [16]byte:
		return uuid.UUID(value), true
	case []byte:
		if len(value) != len(uuid.UUID{}) {
			return uuid.UUID{}, false
		}

		var converted uuid.UUID
		copy(converted[:], value)

		return converted, true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionUUID(literal)
		}

		return uuid.UUID{}, false
	}
}

func convertPartitionDecimal(value any) (Decimal, bool) {
	switch value := value.(type) {
	case Decimal:
		return value, true
	default:
		if literal, ok := partitionLiteralValue(value); ok {
			return convertPartitionDecimal(literal)
		}

		return Decimal{}, false
	}
}

func convertPartitionDate(value any) (Date, bool) {
	converted, ok := convertPartitionInt32(value)

	return Date(converted), ok
}

func convertPartitionTime(value any) (Time, bool) {
	converted, ok := convertPartitionInt64(value)

	return Time(converted), ok
}

func convertPartitionTimestamp(value any) (Timestamp, bool) {
	converted, ok := convertPartitionInt64(value)

	return Timestamp(converted), ok
}

func convertPartitionTimestampNano(value any) (TimestampNano, bool) {
	converted, ok := convertPartitionInt64(value)

	return TimestampNano(converted), ok
}

func newPartitionFieldStat(typ PrimitiveType) (fieldStats, error) {
	switch typ.(type) {
	case UnknownType:
		return &unknownPartitionFieldStats{}, nil
	case BooleanType:
		return makePartitionFieldStats[bool](convertPartitionBool), nil
	case Int32Type:
		return makePartitionFieldStats[int32](convertPartitionInt32), nil
	case Int64Type:
		return makePartitionFieldStats[int64](convertPartitionInt64), nil
	case Float32Type:
		return makePartitionFieldStats[float32](convertPartitionFloat32), nil
	case Float64Type:
		return makePartitionFieldStats[float64](convertPartitionFloat64), nil
	case StringType:
		return makePartitionFieldStats[string](convertPartitionString), nil
	case DateType:
		return makePartitionFieldStats[Date](convertPartitionDate), nil
	case TimeType:
		return makePartitionFieldStats[Time](convertPartitionTime), nil
	case TimestampType:
		return makePartitionFieldStats[Timestamp](convertPartitionTimestamp), nil
	case TimestampTzType:
		return makePartitionFieldStats[Timestamp](convertPartitionTimestamp), nil
	case TimestampNsType, TimestampTzNsType:
		return makePartitionFieldStats[TimestampNano](convertPartitionTimestampNano), nil
	case UUIDType:
		return makePartitionFieldStats[uuid.UUID](convertPartitionUUID), nil
	case BinaryType:
		return makePartitionFieldStats[[]byte](convertPartitionBytes), nil
	case FixedType:
		return makePartitionFieldStats[[]byte](convertPartitionBytes), nil
	case DecimalType:
		return makePartitionFieldStats[Decimal](convertPartitionDecimal), nil
	default:
		return nil, fmt.Errorf("expected primitive type for partition type: %s", typ)
	}
}

func (p *partitionFieldStats[T]) toSummary() FieldSummary {
	var (
		lowerBound *[]byte
		upperBound *[]byte
		lit        Literal
	)

	if p.min != nil {
		lit = NewLiteral(*p.min)
		lb, _ := lit.MarshalBinary()
		lowerBound = &lb
	}

	if p.max != nil {
		lit = NewLiteral(*p.max)
		ub, _ := lit.MarshalBinary()
		upperBound = &ub
	}

	return FieldSummary{
		ContainsNull: p.containsNull,
		ContainsNaN:  &p.containsNan,
		LowerBound:   lowerBound,
		UpperBound:   upperBound,
	}
}

func (p *partitionFieldStats[T]) update(value any) (err error) {
	if value == nil {
		p.containsNull = true

		return err
	}

	actualVal, ok := p.convert(value)
	if !ok {
		return fmt.Errorf("expected type %T, got %T", actualVal, value)
	}

	switch f := any(actualVal).(type) {
	case float32:
		if math.IsNaN(float64(f)) {
			p.containsNan = true

			return err
		}
	case float64:
		if math.IsNaN(f) {
			p.containsNan = true

			return err
		}
	}

	if p.min == nil {
		actualVal = clonePartitionStatValue(actualVal)
		p.min = &actualVal
		p.max = &actualVal
	} else {
		isMin := p.cmp(actualVal, *p.min) < 0
		isMax := p.cmp(actualVal, *p.max) > 0
		if isMin || isMax {
			actualVal = clonePartitionStatValue(actualVal)
		}
		if isMin {
			p.min = &actualVal
		}
		if isMax {
			p.max = &actualVal
		}
	}

	return nil
}

type partitionSummaryStats struct {
	fields []NestedField
	stats  []fieldStats
}

func newPartitionSummaryStats(spec PartitionSpec, schema *Schema) (*partitionSummaryStats, error) {
	fields := spec.PartitionType(schema).FieldList
	stats := make([]fieldStats, len(fields))
	var err error
	for i, field := range fields {
		pt, ok := field.Type.(PrimitiveType)
		if !ok {
			return nil, fmt.Errorf("expected primitive type for partition field, got %s", field.Type)
		}

		stats[i], err = newPartitionFieldStat(pt)
		if err != nil {
			return nil, fmt.Errorf("error constructing field stats for partition %d: %s: %s", i, field.Name, err)
		}
	}

	return &partitionSummaryStats{fields: fields, stats: stats}, nil
}

func (p *partitionSummaryStats) update(partition map[int]any) error {
	for i, field := range p.fields {
		if err := p.stats[i].update(partition[field.ID]); err != nil {
			return fmt.Errorf("error updating field stats for partition %d: %s: %s", i, field.Name, err)
		}
	}

	return nil
}

func (p *partitionSummaryStats) summaries() []FieldSummary {
	summaries := make([]FieldSummary, len(p.stats))
	for i, stat := range p.stats {
		summaries[i] = stat.toSummary()
	}

	return summaries
}

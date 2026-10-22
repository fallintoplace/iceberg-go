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
	"reflect"

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

	cmp Comparator[T]
}

func newPartitionFieldStat(typ PrimitiveType) (fieldStats, error) {
	switch typ.(type) {
	case BooleanType:
		return &partitionFieldStats[bool]{cmp: getComparator[bool]()}, nil
	case Int32Type:
		return &partitionFieldStats[int32]{cmp: getComparator[int32]()}, nil
	case Int64Type:
		return &partitionFieldStats[int64]{cmp: getComparator[int64]()}, nil
	case Float32Type:
		return &partitionFieldStats[float32]{cmp: getComparator[float32]()}, nil
	case Float64Type:
		return &partitionFieldStats[float64]{cmp: getComparator[float64]()}, nil
	case StringType:
		return &partitionFieldStats[string]{cmp: getComparator[string]()}, nil
	case DateType:
		return &partitionFieldStats[Date]{cmp: getComparator[Date]()}, nil
	case TimeType:
		return &partitionFieldStats[Time]{cmp: getComparator[Time]()}, nil
	case TimestampType:
		return &partitionFieldStats[Timestamp]{cmp: getComparator[Timestamp]()}, nil
	case TimestampTzType:
		return &partitionFieldStats[Timestamp]{cmp: getComparator[Timestamp]()}, nil
	case UUIDType:
		return &partitionFieldStats[uuid.UUID]{cmp: getComparator[uuid.UUID]()}, nil
	case BinaryType:
		return &partitionFieldStats[[]byte]{cmp: getComparator[[]byte]()}, nil
	case FixedType:
		return &partitionFieldStats[[]byte]{cmp: getComparator[[]byte]()}, nil
	case DecimalType:
		return &partitionFieldStats[Decimal]{cmp: getComparator[Decimal]()}, nil
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

	var actualVal T
	v := reflect.ValueOf(value)
	if !v.CanConvert(reflect.TypeOf(actualVal)) {
		return fmt.Errorf("expected type %T, got %T", actualVal, value)
	}

	actualVal = v.Convert(reflect.TypeOf(actualVal)).Interface().(T)

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
		p.min = &actualVal
		p.max = &actualVal
	} else {
		if p.cmp(actualVal, *p.min) < 0 {
			p.min = &actualVal
		}

		if p.cmp(actualVal, *p.max) > 0 {
			p.max = &actualVal
		}
	}

	return nil
}

func constructPartitionSummaries(spec PartitionSpec, schema *Schema, partitions []map[int]any) ([]FieldSummary, error) {
	partType := spec.PartitionType(schema)
	fieldStats := make([]fieldStats, len(partType.FieldList))
	var err error
	for i, field := range partType.FieldList {
		pt, ok := field.Type.(PrimitiveType)
		if !ok {
			return nil, fmt.Errorf("expected primitive type for partition field, got %s", field.Type)
		}

		fieldStats[i], err = newPartitionFieldStat(pt)
		if err != nil {
			return nil, fmt.Errorf("error constructing field stats for partition %d: %s: %s", i, field.Name, err)
		}
	}

	for _, part := range partitions {
		for i, field := range partType.FieldList {
			if err := fieldStats[i].update(part[field.ID]); err != nil {
				return nil, fmt.Errorf("error updating field stats for partition %d: %s: %s", i, field.Name, err)
			}
		}
	}

	summaries := make([]FieldSummary, len(fieldStats))
	for i, stat := range fieldStats {
		summaries[i] = stat.toSummary()
	}

	return summaries, nil
}

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

package table

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
)

func interleavedPartitionStringRecord(
	t testing.TB, typ arrow.DataType, distinct, rows int,
) arrow.RecordBatch {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{{Name: "partition_source", Type: typ, Nullable: true}}, nil)
	var col arrow.Array
	switch typ.ID() {
	case arrow.STRING:
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		for row := range rows {
			if row%509 == 0 {
				builder.AppendNull()
			} else {
				builder.Append(fmt.Sprintf("partition-source-%06d", row%distinct))
			}
		}
		col = builder.NewArray()
		builder.Release()
	case arrow.LARGE_STRING:
		builder := array.NewLargeStringBuilder(memory.DefaultAllocator)
		for row := range rows {
			if row%509 == 0 {
				builder.AppendNull()
			} else {
				builder.Append(fmt.Sprintf("partition-source-%06d", row%distinct))
			}
		}
		col = builder.NewArray()
		builder.Release()
	case arrow.BINARY, arrow.LARGE_BINARY:
		builder := array.NewBinaryBuilder(memory.DefaultAllocator, typ.(arrow.BinaryDataType))
		for row := range rows {
			if row%509 == 0 {
				builder.AppendNull()
			} else {
				builder.Append([]byte(fmt.Sprintf("partition-source-%06d", row%distinct)))
			}
		}
		col = builder.NewArray()
		builder.Release()
	default:
		t.Fatalf("unsupported benchmark type: %s", typ)
	}

	record := array.NewRecordBatch(schema, []arrow.Array{col}, int64(rows))
	col.Release()

	return record
}

func interleavedPartitionPlan(
	t testing.TB, schema *arrow.Schema, source iceberg.Type, transform iceberg.Transform,
) *partitionExtractionPlan {
	t.Helper()
	icebergSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "partition_source", Type: source},
	)
	spec := iceberg.NewPartitionSpec(iceberg.PartitionField{
		SourceIDs: []int{1}, FieldID: 1000, Name: "partition_key", Transform: transform,
	})
	plan, err := newPartitionExtractionPlan(spec, icebergSchema, schema)
	if err != nil {
		t.Fatal(err)
	}

	return plan
}

func TestPartitionExtractionInterleavedStringCacheParity(t *testing.T) {
	tests := []struct {
		name      string
		typ       arrow.DataType
		source    iceberg.Type
		transform iceberg.Transform
		distinct  int
	}{
		{"bucket_string", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String, iceberg.BucketTransform{NumBuckets: 64}, 128},
		{"truncate_string", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String, iceberg.TruncateTransform{Width: 18}, 128},
		{"bucket_large_string", arrow.BinaryTypes.LargeString, iceberg.PrimitiveTypes.String, iceberg.BucketTransform{NumBuckets: 64}, 8},
		{"bucket_binary", arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.Binary, iceberg.BucketTransform{NumBuckets: 64}, 128},
		{"truncate_binary", arrow.BinaryTypes.Binary, iceberg.PrimitiveTypes.Binary, iceberg.TruncateTransform{Width: 12}, 128},
		{"bucket_large_binary", arrow.BinaryTypes.LargeBinary, iceberg.PrimitiveTypes.Binary, iceberg.BucketTransform{NumBuckets: 64}, 8},
		{"high_cardinality_fallback", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String, iceberg.BucketTransform{NumBuckets: 64}, 1024},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			record := interleavedPartitionStringRecord(t, tc.typ, tc.distinct, 4096)
			defer record.Release()

			plan := interleavedPartitionPlan(t, record.Schema(), tc.source, tc.transform)
			// Compare against the independent per-row oracle, including
			// partition key equality, group order, and null handling.
			assertPartitionExtractionEquivalent(t, plan, record)
		})
	}
}

func TestPartitionTransformValueCacheBudgetAndNil(t *testing.T) {
	builder := array.NewStringBuilder(memory.DefaultAllocator)
	for i := range maxPartitionTransformCacheEntries + 1 {
		builder.Append(fmt.Sprintf("distinct-%d", i))
	}
	column := builder.NewArray()
	builder.Release()
	defer column.Release()

	cache := &partitionTransformValueCache{}
	calls := 0
	convert := func(_ arrow.Array, row int) (any, error) {
		calls++

		return row, nil
	}
	for row := range column.Len() {
		if _, err := cache.valueAt(column, row, convert); err != nil {
			t.Fatal(err)
		}
	}
	if !cache.disabled || len(cache.values) != 0 || calls != column.Len() {
		t.Fatalf("overflow must disable and release cache: disabled=%t entries=%d calls=%d",
			cache.disabled, len(cache.values), calls)
	}

	nullBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	nullBuilder.Append("same")
	nullBuilder.Append("other")
	nullBuilder.Append("same")
	nullColumn := nullBuilder.NewArray()
	nullBuilder.Release()
	defer nullColumn.Release()

	cache = &partitionTransformValueCache{}
	calls = 0
	nullable := func(_ arrow.Array, _ int) (any, error) {
		calls++

		return nil, nil
	}
	for row := range nullColumn.Len() {
		if value, err := cache.valueAt(nullColumn, row, nullable); err != nil || value != nil {
			t.Fatalf("null result at row %d: %v, %v", row, value, err)
		}
	}
	if calls != 2 {
		t.Fatalf("nil transforms must be cached: got %d calls, want 2", calls)
	}

	largeBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	largeBuilder.Append(strings.Repeat("x", maxPartitionTransformCacheKeySize+1))
	largeColumn := largeBuilder.NewArray()
	largeBuilder.Release()
	defer largeColumn.Release()
	cache = &partitionTransformValueCache{}
	if _, err := cache.valueAt(largeColumn, 0, convert); err != nil {
		t.Fatal(err)
	}
	if len(cache.values) != 0 {
		t.Fatal("oversized values must not be retained in the cache")
	}
}

func BenchmarkPartitionExtractionInterleavedStringCache(b *testing.B) {
	const rows = 65_536

	for _, distinct := range []int{8, 128, 512, 65_536} {
		b.Run(fmt.Sprintf("distinct_%d", distinct), func(b *testing.B) {
			record := interleavedPartitionStringRecord(b, arrow.BinaryTypes.String, distinct, rows)
			defer record.Release()
			plan := interleavedPartitionPlan(b, record.Schema(), iceberg.PrimitiveTypes.String,
				iceberg.BucketTransform{NumBuckets: 64})

			b.Run("per_row_reference", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					result, err := partitionExtractionPerRowForTest(plan, record)
					if err != nil {
						b.Fatal(err)
					}
					partitionCollectionBenchmarkSink = len(result)
				}
			})
			b.Run("cached", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					result, err := plan.getRecordPartitions(record)
					if err != nil {
						b.Fatal(err)
					}
					partitionCollectionBenchmarkSink = len(result)
				}
			})
		})
	}
}

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
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type pairInt64BenchShape struct {
	name            string
	rows            int
	deletes         int
	firstNullEvery  int
	secondNullEvery int
	offset          int
	keyKind         string
}

func buildPairInt64BenchRecord(mem memory.Allocator, shape pairInt64BenchShape) arrow.RecordBatch {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(mem, sc)
	defer builder.Release()

	first := builder.Field(0).(*array.Int64Builder)
	second := builder.Field(1).(*array.Int64Builder)
	for i := range shape.rows + shape.offset {
		row := i - shape.offset
		if row >= 0 && shape.firstNullEvery > 0 && row%shape.firstNullEvery == 0 {
			first.AppendNull()
		} else {
			first.Append(int64(row))
		}
		if row >= 0 && shape.secondNullEvery > 0 && row%shape.secondNullEvery == 0 {
			second.AppendNull()
		} else {
			second.Append(int64(row % 127))
		}
	}

	full := builder.NewRecordBatch()
	if shape.offset == 0 {
		return full
	}

	cols := []arrow.Array{
		array.NewSlice(full.Column(0), int64(shape.offset), int64(shape.rows+shape.offset)),
		array.NewSlice(full.Column(1), int64(shape.offset), int64(shape.rows+shape.offset)),
	}
	full.Release()
	sliced := array.NewRecordBatch(sc, cols, int64(shape.rows))
	for _, col := range cols {
		col.Release()
	}
	return sliced
}

func buildPairInt64BenchKeys(rec arrow.RecordBatch, shape pairInt64BenchShape) set[string] {
	keys := make(set[string], shape.deletes)
	var buf bytes.Buffer
	for i := range shape.deletes {
		buf.Reset()
		switch shape.keyKind {
		case "unmatched":
			buf.WriteByte(1)
			bufPutUint64(&buf, uint64(shape.rows+i+1))
			buf.WriteByte(1)
			bufPutUint64(&buf, uint64(i%127))
		case "null-only":
			buf.WriteByte(0)
			buf.WriteByte(1)
			bufPutUint64(&buf, uint64(i+1_000))
		default:
			row := i * shape.rows / shape.deletes
			encodeArrowValue(&buf, rec.Column(0), row)
			encodeArrowValue(&buf, rec.Column(1), row)
		}
		keys[buf.String()] = struct{}{}
	}
	return keys
}

// The existing benchmark covers non-null rows at multiple batch and delete-set
// sizes. These cases also measure null handling, sliced columns, match density,
// and no-op fast paths. Index construction is deliberately outside the timer.
func BenchmarkProcessEqualityDeletesPairInt64Shapes(b *testing.B) {
	shapes := []pairInt64BenchShape{
		{name: "no-matches", rows: 65_536, deletes: 1_024, keyKind: "unmatched"},
		{name: "sparse-matches", rows: 65_536, deletes: 1_024},
		{name: "dense-matches", rows: 65_536, deletes: 32_768},
		{name: "both-nullable", rows: 65_536, deletes: 8_192, firstNullEvery: 17, secondNullEvery: 11},
		{name: "one-nullable", rows: 65_536, deletes: 8_192, firstNullEvery: 7},
		{name: "sliced-nullable", rows: 65_536, deletes: 8_192, firstNullEvery: 17, secondNullEvery: 11, offset: 7},
		{name: "null-only-keys/no-nulls", rows: 65_536, deletes: 1_024, keyKind: "null-only"},
		{name: "empty-keys", rows: 65_536},
	}

	for _, shape := range shapes {
		b.Run(shape.name, func(b *testing.B) {
			mem := memory.NewGoAllocator()
			rec := buildPairInt64BenchRecord(mem, shape)
			defer rec.Release()
			ctx := compute.WithAllocator(b.Context(), mem)
			keys := buildPairInt64BenchKeys(rec, shape)
			makeDeleteSet := func() *equalityDeleteSet {
				return &equalityDeleteSet{
					keys: keys, fieldIDs: []int{1, 2}, colNames: []string{"id", "category"},
				}
			}
			genericSet := makeDeleteSet()
			genericSet.pairInt64Once.Do(func() {})
			typedSet := makeDeleteSet()

			generic, err := processEqualityDeletesColumnarForFile(
				ctx, []*equalityDeleteSet{genericSet}, benchIntFileSchema(), "bench.parquet")
			if err != nil {
				b.Fatal(err)
			}
			typed, err := processEqualityDeletesColumnarForFile(
				ctx, []*equalityDeleteSet{typedSet}, benchIntFileSchema(), "bench.parquet")
			if err != nil {
				b.Fatal(err)
			}
			if typedSet.pairInt64 == nil {
				b.Fatal("typed index was not built")
			}

			// Reject an optimized path that does less work by returning wrong rows.
			rec.Retain()
			want, err := generic(rec)
			if err != nil {
				b.Fatal(err)
			}
			rec.Retain()
			got, err := typed(rec)
			if err != nil {
				want.Release()
				b.Fatal(err)
			}
			equal := array.RecordEqual(want, got)
			want.Release()
			got.Release()
			if !equal {
				b.Fatal("typed and generic equality deletes returned different records")
			}

			for _, mode := range []struct {
				name   string
				filter recProcessFn
			}{
				{name: "generic", filter: generic},
				{name: "typed", filter: typed},
			} {
				b.Run(mode.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64(shape.rows) * 16)
					b.ResetTimer()
					for b.Loop() {
						rec.Retain()
						result, err := mode.filter(rec)
						if err != nil {
							b.Fatal(err)
						}
						result.Release()
					}
				})
			}
		})
	}
}

var pairInt64IndexBenchmarkSink *pairInt64EqualityDeleteSet

// Measures the extra one-time hash-index build cost, excluded from scan
// benchmarks. Null-only keys should not allocate an unused non-null map.
func BenchmarkPairInt64IndexBuild(b *testing.B) {
	for _, kind := range []string{"non-null", "first-null"} {
		for _, keyCount := range []int{10, 1_000, 10_000} {
			b.Run(fmt.Sprintf("%s/keys=%d", kind, keyCount), func(b *testing.B) {
				keys := make(set[string], keyCount)
				var buf bytes.Buffer
				for i := range keyCount {
					buf.Reset()
					if kind == "first-null" {
						buf.WriteByte(0)
					} else {
						buf.WriteByte(1)
						bufPutUint64(&buf, uint64(i))
					}
					buf.WriteByte(1)
					bufPutUint64(&buf, uint64(i+1))
					keys[buf.String()] = struct{}{}
				}
				schema := benchIntFileSchema()
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					del := &equalityDeleteSet{keys: keys, fieldIDs: []int{1, 2}}
					pairInt64IndexBenchmarkSink = del.pairInt64Set(schema)
				}
			})
		}
	}
}

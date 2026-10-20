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
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var benchmarkLeadingInt64Rows int64

// Build real Arrow records with a controlled fraction of delete-key matches.
func benchmarkCompositeInt64Rows(mem memory.Allocator, rows, keyCount, matchPercent int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()
	ids := builder.Field(0).(*array.Int64Builder)
	categories := builder.Field(1).(*array.Int64Builder)
	for i := range rows {
		if i%100 < matchPercent {
			key := (i / 100) % keyCount
			ids.Append(int64(key * 3))
			categories.Append(int64((key * 3) % 100))
		} else {
			ids.Append(int64(1_000_000 + i))
			categories.Append(int64(i % 100))
		}
	}
	return builder.NewRecordBatch()
}

// go test ./table -run '^$' -bench '^BenchmarkLeadingInt64EqualityDeletePrefix$' -benchmem -benchtime=500ms -count=5 -cpu=1
func BenchmarkLeadingInt64EqualityDeletePrefix(b *testing.B) {
	for _, tc := range []struct {
		rows         int
		keys         int
		matchPercent int
	}{
		{100_000, 100, 0},
		{1_000_000, 100, 0},
		{1_000_000, 100, 1},
		{1_000_000, 10_000, 1},
		{1_000_000, 100, 50},
		{1_000_000, 10_000, 50},
	} {
		b.Run(fmt.Sprintf("rows=%d/keys=%d/matches=%d%%", tc.rows, tc.keys, tc.matchPercent), func(b *testing.B) {
			for _, optimized := range []bool{false, true} {
				name := "generic"
				if optimized {
					name = "prefix"
				}
				b.Run(name, func(b *testing.B) {
					ctx := compute.WithAllocator(context.Background(), memory.DefaultAllocator)
					record := benchmarkCompositeInt64Rows(memory.DefaultAllocator, tc.rows, tc.keys, tc.matchPercent)
					defer record.Release()
					deleteSet := buildBenchDeleteSetInt(tc.keys)
					if !optimized {
						deleteSet.leadingInt64Once.Do(func() {})
					}
					filter, err := processEqualityDeletesColumnarForFile(ctx, []*equalityDeleteSet{deleteSet}, benchIntFileSchema(), "benchmark.parquet")
					if err != nil {
						b.Fatal(err)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						record.Retain()
						result, err := filter(record)
						if err != nil {
							b.Fatal(err)
						}
						benchmarkLeadingInt64Rows = result.NumRows()
						result.Release()
					}
				})
			}
		})
	}
}

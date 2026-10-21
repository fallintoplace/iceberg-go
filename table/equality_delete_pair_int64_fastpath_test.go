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
	"math"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessEqualityDeletesPairInt64MatchesGeneric(t *testing.T) {
	type values struct {
		first, second int64
		nullFirst, nullSecond bool
	}
	rows := []values{
		{first: 0, second: 0},
		{first: 1, second: 2},
		{first: 0, second: 2, nullFirst: true},
		{first: 1, second: 0, nullSecond: true},
		{nullFirst: true, nullSecond: true},
		{first: -1, second: math.MaxInt64},
		{first: 1, second: 2},
		{first: math.MinInt64, second: -5},
	}
	cases := []struct {
		name string
		deleteRows []int
	}{
		{name: "no_match"},
		{name: "zero_pair", deleteRows: []int{0}},
		{name: "duplicate_and_negative", deleteRows: []int{1, 5, 7}},
		{name: "first_null", deleteRows: []int{2}},
		{name: "second_null", deleteRows: []int{3}},
		{name: "both_null", deleteRows: []int{4}},
		{name: "all_shapes", deleteRows: []int{0, 1, 2, 3, 4, 5, 7}},
	}

	for _, offset := range []int{0, 3, 9} {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("offset=%d/%s", offset, tc.name), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				ctx := compute.WithAllocator(t.Context(), mem)

				arrowSchema := arrow.NewSchema([]arrow.Field{
					{Name: "first", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "second", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				}, nil)
				builder := array.NewRecordBuilder(mem, arrowSchema)
				firstBuilder := builder.Field(0).(*array.Int64Builder)
				secondBuilder := builder.Field(1).(*array.Int64Builder)
				for range offset {
					firstBuilder.Append(999)
					secondBuilder.Append(999)
				}
				for _, row := range rows {
					if row.nullFirst {
						firstBuilder.AppendNull()
					} else {
						firstBuilder.Append(row.first)
					}
					if row.nullSecond {
						secondBuilder.AppendNull()
					} else {
						secondBuilder.Append(row.second)
					}
				}
				full := builder.NewRecordBatch()
				builder.Release()

				firstSlice := array.NewSlice(full.Column(0), int64(offset), int64(offset+len(rows)))
				secondSlice := array.NewSlice(full.Column(1), int64(offset), int64(offset+len(rows)))
				full.Release()
				record := array.NewRecordBatch(arrowSchema, []arrow.Array{firstSlice, secondSlice}, int64(len(rows)))
				firstSlice.Release()
				secondSlice.Release()
				defer record.Release()

				keys := make(set[string], len(tc.deleteRows))
				for _, row := range tc.deleteRows {
					var encoded bytes.Buffer
					encodeArrowValue(&encoded, record.Column(0), row)
					encodeArrowValue(&encoded, record.Column(1), row)
					keys[encoded.String()] = struct{}{}
				}
				fastSet := &equalityDeleteSet{keys: keys, fieldIDs: []int{1, 2}, colNames: []string{"first", "second"}}
				genericSet := &equalityDeleteSet{keys: keys, fieldIDs: []int{1, 2}, colNames: []string{"first", "second"}}
				genericSet.pairInt64Once.Do(func() {})

				fileSchema := iceberg.NewSchema(0,
					iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int64},
					iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Int64})
				generic, err := processEqualityDeletesColumnarForFile(ctx,
					[]*equalityDeleteSet{genericSet}, fileSchema, "data.parquet")
				require.NoError(t, err)
				fast, err := processEqualityDeletesColumnarForFile(ctx,
					[]*equalityDeleteSet{fastSet}, fileSchema, "data.parquet")
				require.NoError(t, err)
				require.NotNil(t, fastSet.pairInt64)
				require.Nil(t, genericSet.pairInt64)

				record.Retain()
				want, err := generic(record)
				require.NoError(t, err)
				defer want.Release()
				record.Retain()
				got, err := fast(record)
				require.NoError(t, err)
				defer got.Release()

				assert.True(t, array.RecordEqual(want, got), "fast and generic equality deletes differ")
				if len(tc.deleteRows) == 0 {
					assert.Same(t, record, got)
				}
			})
		}
	}
}

func TestPairInt64EqualityDeleteInvalidEncodedKeysFallBack(t *testing.T) {
	for _, key := range []string{
		"",
		"\x00",
		"\x01",
		"\x02",
		"\x01\x00\x00\x00\x00\x00\x00\x00\x00",
		"\x00\x02",
		"\x00\x00\x00",
	} {
		t.Run(fmt.Sprintf("%x", key), func(t *testing.T) {
			deleteSet := &equalityDeleteSet{
				keys:     set[string]{key: {}},
				fieldIDs: []int{1, 2},
				colNames: []string{"first", "second"},
			}
			schema := iceberg.NewSchema(0,
				iceberg.NestedField{ID: 1, Name: "first", Type: iceberg.PrimitiveTypes.Int64},
				iceberg.NestedField{ID: 2, Name: "second", Type: iceberg.PrimitiveTypes.Int64})
			_, err := processEqualityDeletesColumnarForFile(t.Context(),
				[]*equalityDeleteSet{deleteSet}, schema, "data.parquet")
			require.NoError(t, err)
			assert.Nil(t, deleteSet.pairInt64)
		})
	}
}

func TestPairInt64EqualityDeleteIndexConcurrent(t *testing.T) {
	deleteSet := buildBenchDeleteSetInt(1_000)
	schema := benchIntFileSchema()

	const workers = 32
	var wg sync.WaitGroup
	indices := make([]*pairInt64EqualityDeleteSet, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			indices[i] = deleteSet.pairInt64Set(schema)
		}()
	}
	wg.Wait()
	require.NotNil(t, indices[0])
	require.Len(t, indices[0].keys, 1_000)
	for _, result := range indices[1:] {
		assert.Same(t, indices[0], result)
	}
}

func BenchmarkProcessEqualityDeletesPairInt64(b *testing.B) {
	for _, fast := range []bool{false, true} {
		mode := "generic"
		if fast {
			mode = "typed_pair"
		}
		b.Run(mode, func(b *testing.B) {
			benchEqDeletesForFile(b, buildBenchRecordInt, func(n int) *equalityDeleteSet {
				deleteSet := buildBenchDeleteSetInt(n)
				if !fast {
					deleteSet.pairInt64Once.Do(func() {})
				}

				return deleteSet
			}, benchIntFileSchema())
		})
	}
}

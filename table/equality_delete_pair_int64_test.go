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
	iceSchema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "tenant", Type: iceberg.PrimitiveTypes.Int64},
	)
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "tenant", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	rows := []struct {
		id, tenant           int64
		idValid, tenantValid bool
	}{
		{math.MinInt64, math.MaxInt64, true, true},
		{1, 2, true, true},
		{0, 4, false, true},
		{3, 0, true, false},
		{0, 0, false, false},
		{math.MaxInt64, math.MinInt64, true, true},
		{-1, -2, true, true},
		{1, 2, true, true},
	}

	for _, offset := range []int{0, 3, 9} {
		for _, tc := range []struct {
			name         string
			deleteRows   []int
			addMissing   bool
			addMalformed bool
			wantRows     int64
		}{
			{name: "all null shapes and duplicates", deleteRows: []int{0, 2, 3, 4, 7}, wantRows: 2},
			{name: "only non-null keys", deleteRows: []int{0, 7}, wantRows: 5},
			{name: "only nullable keys", deleteRows: []int{2, 3, 4}, wantRows: 5},
			{name: "no matches", addMissing: true, wantRows: 8},
			{name: "empty key set", wantRows: 8},
			{name: "malformed key falls back", deleteRows: []int{7}, addMalformed: true, wantRows: 6},
		} {
			t.Run(fmt.Sprintf("%s/offset=%d", tc.name, offset), func(t *testing.T) {
				mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
				defer mem.AssertSize(t, 0)
				ctx := compute.WithAllocator(t.Context(), mem)

				builder := array.NewRecordBuilder(mem, arrowSchema)
				idBuilder := builder.Field(0).(*array.Int64Builder)
				tenantBuilder := builder.Field(1).(*array.Int64Builder)
				for range offset {
					idBuilder.Append(100)
					tenantBuilder.Append(100)
				}
				for _, row := range rows {
					if row.idValid {
						idBuilder.Append(row.id)
					} else {
						idBuilder.AppendNull()
					}
					if row.tenantValid {
						tenantBuilder.Append(row.tenant)
					} else {
						tenantBuilder.AppendNull()
					}
				}
				full := builder.NewRecordBatch()
				builder.Release()
				sliced := []arrow.Array{
					array.NewSlice(full.Column(0), int64(offset), int64(offset+len(rows))),
					array.NewSlice(full.Column(1), int64(offset), int64(offset+len(rows))),
				}
				full.Release()
				record := array.NewRecordBatch(arrowSchema, sliced, int64(len(rows)))
				for _, col := range sliced {
					col.Release()
				}
				defer record.Release()

				keys := make(set[string])
				for _, row := range tc.deleteRows {
					var buf bytes.Buffer
					encodeArrowValue(&buf, record.Column(0), row)
					encodeArrowValue(&buf, record.Column(1), row)
					keys[buf.String()] = struct{}{}
				}
				if tc.addMissing {
					var buf bytes.Buffer
					buf.WriteByte(1)
					bufPutUint64(&buf, 12345)
					buf.WriteByte(1)
					bufPutUint64(&buf, 67890)
					keys[buf.String()] = struct{}{}
				}
				if tc.addMalformed {
					keys["\x01\x00"] = struct{}{}
				}

				fastSet := &equalityDeleteSet{
					keys: keys, fieldIDs: []int{1, 2}, colNames: []string{"id", "tenant"},
				}
				genericSet := &equalityDeleteSet{
					keys: keys, fieldIDs: []int{1, 2}, colNames: []string{"id", "tenant"},
				}
				// Keep the same schema and input keys but disable the optional index.
				genericSet.pairInt64Once.Do(func() {})

				generic, err := processEqualityDeletesColumnarForFile(
					ctx, []*equalityDeleteSet{genericSet}, iceSchema, "data.parquet")
				require.NoError(t, err)
				fast, err := processEqualityDeletesColumnarForFile(
					ctx, []*equalityDeleteSet{fastSet}, iceSchema, "data.parquet")
				require.NoError(t, err)
				require.Nil(t, genericSet.pairInt64)
				if tc.addMalformed {
					require.Nil(t, fastSet.pairInt64, "invalid encoded keys must use the generic path")
				} else {
					require.NotNil(t, fastSet.pairInt64, "the typed path must be exercised")
				}

				record.Retain()
				want, err := generic(record)
				require.NoError(t, err)
				defer want.Release()
				record.Retain()
				got, err := fast(record)
				require.NoError(t, err)
				defer got.Release()

				assert.True(t, array.RecordEqual(want, got), "generic: %s; typed: %s", want, got)
				assert.Equal(t, tc.wantRows, got.NumRows())
				if tc.wantRows == record.NumRows() {
					assert.Same(t, record, got)
				}
			})
		}
	}
}

func TestPairInt64IndexRejectsInvalidKeys(t *testing.T) {
	for _, key := range []string{
		"",
		"\x01",
		"\x01\x00",
		"\x00",
		"\x02\x00",
		"\x00\x01\x00",
		"\x00\x00extra",
	} {
		t.Run(fmt.Sprintf("key=%x", key), func(t *testing.T) {
			e := &equalityDeleteSet{
				keys: set[string]{key: {}}, fieldIDs: []int{1, 2},
			}
			schema := benchIntFileSchema()
			assert.Nil(t, e.pairInt64Set(schema))
		})
	}
}

func TestPairInt64IndexSkipsNestedAndNonInt64Fields(t *testing.T) {
	nested := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 3, Name: "parent", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			},
		}},
		iceberg.NestedField{ID: 2, Name: "tenant", Type: iceberg.PrimitiveTypes.Int64},
	)
	var buf bytes.Buffer
	buf.WriteByte(1)
	bufPutUint64(&buf, 12)
	buf.WriteByte(1)
	bufPutUint64(&buf, 34)
	nestedSet := &equalityDeleteSet{
		keys: set[string]{buf.String(): {}},
		fieldIDs: []int{1, 2}, colNames: []string{"parent.id", "tenant"},
	}
	_, err := processEqualityDeletesColumnarForFile(
		t.Context(), []*equalityDeleteSet{nestedSet}, nested, "data.parquet")
	require.NoError(t, err)
	assert.Nil(t, nestedSet.pairInt64, "nested field needs parent null handling")

	nonInt64 := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "tenant", Type: iceberg.PrimitiveTypes.Int32},
	)
	assert.Nil(t, nestedSet.pairInt64Set(nonInt64))
}

func TestPairInt64IndexConcurrentBuild(t *testing.T) {
	e := buildBenchDeleteSetInt(1_000)
	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	indexes := make([]*pairInt64EqualityDeleteSet, workers)
	for i := range indexes {
		go func() {
			defer wg.Done()
			indexes[i] = e.pairInt64Set(benchIntFileSchema())
		}()
	}
	wg.Wait()
	require.NotNil(t, indexes[0])
	for _, index := range indexes[1:] {
		assert.Same(t, indexes[0], index)
	}
}

func TestPairInt64IndexSkipsUnusedNonNullMap(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	rec := buildBenchRecordInt(mem, 128)
	defer rec.Release()

	for _, tc := range []struct {
		name string
		keys set[string]
	}{
		{name: "empty", keys: make(set[string])},
		{name: "first-null-only", keys: func() set[string] {
			keys := make(set[string], 100)
			var buf bytes.Buffer
			for i := range 100 {
				buf.Reset()
				buf.WriteByte(0)
				buf.WriteByte(1)
				bufPutUint64(&buf, uint64(i))
				keys[buf.String()] = struct{}{}
			}
			return keys
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			del := &equalityDeleteSet{keys: tc.keys, fieldIDs: []int{1, 2}}
			idx := del.pairInt64Set(benchIntFileSchema())
			require.NotNil(t, idx)
			assert.Nil(t, idx.keys, "non-null map should stay unallocated")

			var mask equalityDeleteMask
			defer mask.release()
			assert.True(t, mask.applyPairInt64(rec.Column(0), rec.Column(1), idx, mem))
			assert.Nil(t, mask.buffer, "no rows can match")
		})
	}
}

func TestProcessEqualityDeletesPairInt64MultipleSets(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	rec := buildBenchRecordInt(mem, 10)
	defer rec.Release()

	buildSets := func(useGeneric bool) []*equalityDeleteSet {
		var sets []*equalityDeleteSet
		for _, deletedRows := range [][]int{{1, 3, 5}, {3, 7}} {
			keys := make(set[string], len(deletedRows))
			var buf bytes.Buffer
			for _, row := range deletedRows {
				buf.Reset()
				encodeArrowValue(&buf, rec.Column(0), row)
				encodeArrowValue(&buf, rec.Column(1), row)
				keys[buf.String()] = struct{}{}
			}
			del := &equalityDeleteSet{
				keys: keys, fieldIDs: []int{1, 2}, colNames: []string{"id", "category"},
			}
			if useGeneric {
				del.pairInt64Once.Do(func() {})
			}
			sets = append(sets, del)
		}
		return sets
	}
	ctx := compute.WithAllocator(t.Context(), mem)
	generic, err := processEqualityDeletesColumnarForFile(
		ctx, buildSets(true), benchIntFileSchema(), "data.parquet")
	require.NoError(t, err)
	typed, err := processEqualityDeletesColumnarForFile(
		ctx, buildSets(false), benchIntFileSchema(), "data.parquet")
	require.NoError(t, err)

	rec.Retain()
	want, err := generic(rec)
	require.NoError(t, err)
	defer want.Release()
	rec.Retain()
	got, err := typed(rec)
	require.NoError(t, err)
	defer got.Release()

	assert.True(t, array.RecordEqual(want, got))
	assert.EqualValues(t, 6, got.NumRows())
}

func BenchmarkProcessEqualityDeletesPairInt64(b *testing.B) {
	for _, mode := range []string{"generic", "typed"} {
		b.Run(mode, func(b *testing.B) {
			benchEqDeletesForFile(b,
				buildBenchRecordInt,
				func(numDeletes int) *equalityDeleteSet {
					del := buildBenchDeleteSetInt(numDeletes)
					if mode == "generic" {
						del.pairInt64Once.Do(func() {})
					}
					return del
				},
				benchIntFileSchema(),
			)
		})
	}
}

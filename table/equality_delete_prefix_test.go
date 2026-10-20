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
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestLeadingInt64PrefixSet(t *testing.T) {
	var key bytes.Buffer
	negative := int64(-17)
	key.WriteByte(1)
	bufPutUint64(&key, uint64(negative))
	key.WriteByte(0) // null second field
	negativeKey := key.String()
	key.Reset()
	key.WriteByte(0) // null first field
	key.WriteByte(1)
	bufPutUint64(&key, 42)
	nullKey := key.String()

	set := &equalityDeleteSet{
		keys: set[string]{negativeKey: {}, nullKey: {}},
		fieldIDs: []int{1, 2},
	}
	index := set.leadingInt64PrefixSet()
	require.NotNil(t, index)
	require.Contains(t, index.values, uint64(negative))
	require.True(t, index.hasNull)
	require.Same(t, index, set.leadingInt64PrefixSet())

	for _, key := range []string{"", "\x00", "\x01", "\x02\x00", "\x01\x00\x00\x00\x00\x00\x00\x00\x00\x02"} {
		malformed := &equalityDeleteSet{
			keys: set[string]{key: {}},
			fieldIDs: []int{1, 2},
		}
		require.Nil(t, malformed.leadingInt64PrefixSet(), "key %q", key)
	}
	require.Nil(t, (&equalityDeleteSet{
		keys: set[string]{negativeKey: {}},
		fieldIDs: []int{1},
	}).leadingInt64PrefixSet())
}

func compareLeadingInt64Prefix(
	t *testing.T, record arrow.RecordBatch, deleteSet *equalityDeleteSet,
	fileSchema *iceberg.Schema, expectedRows int64,
) {
	t.Helper()
	generic := &equalityDeleteSet{
		keys: deleteSet.keys, fieldIDs: deleteSet.fieldIDs, colNames: deleteSet.colNames,
	}
	generic.leadingInt64Once.Do(func() {}) // canonical path, no prefilter

	ctx := compute.WithAllocator(context.Background(), memory.DefaultAllocator)
	baseFn, err := processEqualityDeletesColumnarForFile(ctx, []*equalityDeleteSet{generic}, fileSchema, "data.parquet")
	require.NoError(t, err)
	fastFn, err := processEqualityDeletesColumnarForFile(ctx, []*equalityDeleteSet{deleteSet}, fileSchema, "data.parquet")
	require.NoError(t, err)

	record.Retain()
	want, err := baseFn(record)
	require.NoError(t, err)
	defer want.Release()
	record.Retain()
	got, err := fastFn(record)
	require.NoError(t, err)
	defer got.Release()
	require.True(t, array.RecordEqual(want, got), "generic: %s; optimized: %s", want, got)
	if expectedRows >= 0 {
		require.Equal(t, expectedRows, got.NumRows())
	}
}

func TestLeadingInt64PrefixMatchesGeneric(t *testing.T) {
	for _, tc := range []struct {
		name        string
		makeRecord  func(memory.Allocator, int) arrow.RecordBatch
		makeDeletes func(int) *equalityDeleteSet
		schema      *iceberg.Schema
	}{
		{"int64_pair", buildBenchRecordInt, buildBenchDeleteSetInt, benchIntFileSchema()},
		{"int64_pair_no_match", buildBenchRecordInt, buildBenchDeleteSetIntNoMatch, benchIntFileSchema()},
		{"int64_string", buildBenchRecordString, buildBenchDeleteSetString, benchStringFileSchema()},
		{"int64_string_no_match", buildBenchRecordString, buildBenchDeleteSetStringNoMatch, benchStringFileSchema()},
		{"nullable_first", buildBenchRecordNullableInt, buildBenchDeleteSetInt, benchIntFileSchema()},
		{"nullable_first_matches", buildBenchRecordNullableInt, func(n int) *equalityDeleteSet {
			set := buildBenchDeleteSetInt(n)
			var key bytes.Buffer
			key.WriteByte(0)
			key.WriteByte(1)
			bufPutUint64(&key, 0)
			set.keys[key.String()] = struct{}{}
			return set
		}, benchIntFileSchema()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := tc.makeRecord(memory.DefaultAllocator, 128)
			defer record.Release()
			compareLeadingInt64Prefix(t, record, tc.makeDeletes(24), tc.schema, -1)
		})
	}
}

func TestLeadingInt64PrefixSliceAndSecondNull(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "category", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	ids := builder.Field(0).(*array.Int64Builder)
	cats := builder.Field(1).(*array.Int64Builder)
	for i := range 16 {
		ids.Append(int64(i))
		if i == 6 {
			cats.AppendNull()
		} else {
			cats.Append(int64(i % 4))
		}
	}
	full := builder.NewRecordBatch()
	defer full.Release()
	idsSlice := array.NewSlice(full.Column(0), 3, 12)
	catsSlice := array.NewSlice(full.Column(1), 3, 12)
	record := array.NewRecordBatch(schema, []arrow.Array{idsSlice, catsSlice}, 9)
	idsSlice.Release()
	catsSlice.Release()
	defer record.Release()

	var key bytes.Buffer
	key.WriteByte(1)
	bufPutUint64(&key, 6)
	key.WriteByte(0)
	deleteSet := &equalityDeleteSet{
		keys: set[string]{key.String(): {}},
		fieldIDs: []int{1, 2},
		colNames: []string{"id", "category"},
	}
	compareLeadingInt64Prefix(t, record, deleteSet, benchIntFileSchema(), 8)
}

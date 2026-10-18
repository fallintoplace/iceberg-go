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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func dvSparseTestBatch(start, count int64) arrow.RecordBatch {
	bldr := array.NewInt64Builder(memory.DefaultAllocator)
	defer bldr.Release()
	for idx := range count {
		bldr.Append(start + idx)
	}
	col := bldr.NewInt64Array()
	defer col.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: arrow.PrimitiveTypes.Int64}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{col}, count)
}

func TestFilterByDeletionVectorSparseBatches(t *testing.T) {
	const (
		rowsPerBatch = int64(16)
		totalRows    = int64(2048)
	)
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(83)                    // Batch 5, row 3.
	bitmap.Set(319)                   // Batch 19, last row.
	bitmap.Set(uint64(totalRows + 4)) // Out-of-range positions are ignored.

	filter := filterByDeletionVector(context.Background(), bitmap, totalRows, (&rowPositionSource{}).cursor())
	for batchIdx := int64(0); batchIdx < totalRows/rowsPerBatch; batchIdx++ {
		start := batchIdx * rowsPerBatch
		input := dvSparseTestBatch(start, rowsPerBatch)
		output, err := filter(input)
		require.NoError(t, err)

		want := make([]int64, 0, rowsPerBatch)
		for value := start; value < start+rowsPerBatch; value++ {
			if value != 83 && value != 319 {
				want = append(want, value)
			}
		}
		require.Equal(t, want, output.Column(0).(*array.Int64).Int64Values())
		if batchIdx != 5 && batchIdx != 19 {
			require.Same(t, input, output, "clean batches should pass through without filtering")
		}
		output.Release()
	}
}

func TestFilterByDeletionVectorSparseClustered(t *testing.T) {
	const rowCount int64 = 4096
	bitmap := dv.NewRoaringPositionBitmap()
	for _, pos := range []uint64{0, 1, 2, 15, 30, 31} {
		bitmap.Set(pos)
	}
	filter := filterByDeletionVector(context.Background(), bitmap, rowCount, (&rowPositionSource{}).cursor())

	first, err := filter(dvSparseTestBatch(0, 32))
	require.NoError(t, err)
	want := make([]int64, 0, 26)
	for pos := int64(0); pos < 32; pos++ {
		if !bitmap.Contains(uint64(pos)) {
			want = append(want, pos)
		}
	}
	require.Equal(t, want, first.Column(0).(*array.Int64).Int64Values())
	first.Release()

	clean := dvSparseTestBatch(32, 32)
	second, err := filter(clean)
	require.NoError(t, err)
	require.Same(t, clean, second)
	second.Release()
}

func TestFilterByDeletionVectorDenseFallback(t *testing.T) {
	const rowCount int64 = 256
	bitmap := dv.NewRoaringPositionBitmap()
	for pos := uint64(0); pos < uint64(rowCount); pos += 4 {
		bitmap.Set(pos)
	}
	filter := filterByDeletionVector(context.Background(), bitmap, rowCount, (&rowPositionSource{}).cursor())

	for start := int64(0); start < rowCount; start += 16 {
		output, err := filter(dvSparseTestBatch(start, 16))
		require.NoError(t, err)
		want := make([]int64, 0, 12)
		for pos := start; pos < start+16; pos++ {
			if pos%4 != 0 {
				want = append(want, pos)
			}
		}
		require.Equal(t, want, output.Column(0).(*array.Int64).Int64Values())
		output.Release()
	}
}

func TestFilterByDeletionVectorSparseStaleCount(t *testing.T) {
	const rowCount int64 = 1024
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(16)
	filter := filterByDeletionVector(context.Background(), bitmap, rowCount, (&rowPositionSource{}).cursor())

	clean := dvSparseTestBatch(0, 8)
	first, err := filter(clean)
	require.NoError(t, err)
	require.Same(t, clean, first)
	first.Release()

	// The physical batch crosses the manifest rowCount boundary.
	second, err := filter(dvSparseTestBatch(8, 1024))
	require.NoError(t, err)
	defer second.Release()
	values := second.Column(0).(*array.Int64).Int64Values()
	require.Len(t, values, 1023)
	require.Equal(t, int64(8), values[0])
	require.Equal(t, int64(15), values[7])
	require.Equal(t, int64(17), values[8])
	require.Equal(t, int64(1031), values[len(values)-1])
}

func TestFilterByDeletionVectorPrunedRowGroups(t *testing.T) {
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(12)
	bitmap.Set(101)
	source := &rowPositionSource{spans: []tblutils.RowGroupSpan{
		{FirstRowPos: 10, NumRows: 3},
		{FirstRowPos: 100, NumRows: 3},
	}}
	filter := filterByDeletionVector(context.Background(), bitmap, 1024, source.cursor())
	first, err := filter(dvSparseTestBatch(10, 3))
	require.NoError(t, err)
	require.Equal(t, []int64{10, 11}, first.Column(0).(*array.Int64).Int64Values())
	first.Release()
	second, err := filter(dvSparseTestBatch(100, 3))
	require.NoError(t, err)
	require.Equal(t, []int64{100, 102}, second.Column(0).(*array.Int64).Int64Values())
	second.Release()
}

func TestFilterByDeletionVectorSparseVariableBatchSizes(t *testing.T) {
	const rowCount int64 = 2048
	bitmap := dv.NewRoaringPositionBitmap()
	bitmap.Set(100)
	bitmap.Set(1900)
	filter := filterByDeletionVector(context.Background(), bitmap, rowCount, (&rowPositionSource{}).cursor())

	// The first small batch qualifies for sparse filtering, but the
	// subsequent large batch triggers the original full-mask path.
	first, err := filter(dvSparseTestBatch(0, 8))
	require.NoError(t, err)
	first.Release()
	second, err := filter(dvSparseTestBatch(8, 2040))
	require.NoError(t, err)
	defer second.Release()
	values := second.Column(0).(*array.Int64).Int64Values()
	require.Len(t, values, 2038)
	require.Equal(t, int64(99), values[91])
	require.Equal(t, int64(101), values[92])
	require.Equal(t, int64(1901), values[1891])
}

func TestSparseDeletionVectorIndexMemoryBudget(t *testing.T) {
	require.True(t, sparseDeletionVectorWorthIndexing(100, 100_663_296, 8_192))
	require.True(t, sparseDeletionVectorWorthIndexing(10_000, 10_000_000, 1))
	require.False(t, sparseDeletionVectorWorthIndexing(20_000, 10_000_000, 1))
	require.False(t, sparseDeletionVectorWorthIndexing(100, 1_024, 1))
	require.False(t, sparseDeletionVectorWorthIndexing(0, 0, 1))
	require.False(t, sparseDeletionVectorWorthIndexing(0, 1_024, 0))
}

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
	"github.com/apache/iceberg-go/table/dv"
)

// filterByDeletionVectorEagerBenchmark isolates the prior contiguous fast path.
// Both variants include their mask preparation and the real Arrow filter, using
// identical record batches, positions, and allocator settings.
func filterByDeletionVectorEagerBenchmark(ctx context.Context, bitmap *dv.RoaringPositionBitmap, rowCount int64) recProcessFn {
	nextIdx := int64(0)
	keepBits := bitmap.KeepMaskBytes(rowCount)
	buf := memory.NewBufferBytes(keepBits)
	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()
		start := nextIdx
		nextIdx += r.NumRows()
		full := array.NewBoolean(int(rowCount), buf, nil, 0)
		defer full.Release()
		sliced := array.NewSlice(full, start, nextIdx).(*array.Boolean)
		defer sliced.Release()
		return compute.FilterRecordBatch(ctx, r, sliced, compute.DefaultFilterOptions())
	}
}

func BenchmarkFilterByDeletionVectorSparse(b *testing.B) {
	for _, tc := range []struct {
		name       string
		totalRows  int64
		batchRows  int64
		deletions  int
		clustered  bool
	}{
		{name: "1M_sparse", totalRows: 1_048_576, batchRows: 8_192, deletions: 10},
		{name: "10M_sparse", totalRows: 10_485_760, batchRows: 8_192, deletions: 100},
		{name: "100M_sparse", totalRows: 100_663_296, batchRows: 8_192, deletions: 100},
		{name: "10M_sparse_wide_batch", totalRows: 10_485_760, batchRows: 65_536, deletions: 10},
		{name: "10M_clustered", totalRows: 10_485_760, batchRows: 8_192, deletions: 100, clustered: true},
		{name: "10M_medium", totalRows: 10_485_760, batchRows: 8_192, deletions: 10_000},
		{name: "10M_dense", totalRows: 10_485_760, batchRows: 8_192, deletions: 100_000},
	} {
		b.Run(fmt.Sprintf("%s/deletes=%d", tc.name, tc.deletions), func(b *testing.B) {
			bitmap := dv.NewRoaringPositionBitmap()
			for i := range tc.deletions {
				pos := uint64(i) * uint64(tc.totalRows) / uint64(tc.deletions)
				if tc.clustered {
					pos = uint64(i)
				}
				bitmap.Set(pos)
			}
			batch := dvSparseTestBatch(0, tc.batchRows)
			defer batch.Release()
			ctx := context.Background()

			for _, variant := range []struct {
				name  string
				build func() recProcessFn
			}{
				{name: "before_eager", build: func() recProcessFn {
					return filterByDeletionVectorEagerBenchmark(ctx, bitmap, tc.totalRows)
				}},
				{name: "after_adaptive", build: func() recProcessFn {
					return filterByDeletionVector(ctx, bitmap, tc.totalRows, (&rowPositionSource{}).cursor())
				}},
			} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						filter := variant.build()
						for range tc.totalRows / tc.batchRows {
							batch.Retain()
							out, err := filter(batch)
							if err != nil {
								b.Fatal(err)
							}
							out.Release()
						}
					}
				})
			}
		})
	}
}

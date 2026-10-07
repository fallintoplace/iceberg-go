// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/compute"
)

var partitionGatherBenchmarkRows int64

func BenchmarkPartitionGatherPrototype(b *testing.B) {
	const rows = 65_536

	for _, columns := range []int{8, 32, 64} {
		record := newPartitionBatchBenchmarkRecord(columns, rows)
		b.Run(fmt.Sprintf("%d_columns", columns), func(b *testing.B) {
			for _, partitionCount := range []int{64, 256, 1024} {
				partitions := roundRobinPartitionRows(rows, partitionCount)
				b.Run(fmt.Sprintf("%d_partitions", partitionCount), func(b *testing.B) {
					b.Run("current", func(b *testing.B) {
						partitionBatch := partitionBatchByKey(context.Background())
						b.ReportAllocs()
						b.ResetTimer()

						for b.Loop() {
							var seen int64
							for _, indices := range partitions {
								batch, err := partitionBatch(record, indices)
								if err != nil {
									b.Fatal(err)
								}
								seen += batch.NumRows()
								batch.Release()
							}
							partitionGatherBenchmarkRows = seen
						}
					})

					b.Run("serial_take", func(b *testing.B) {
						execCtx := compute.GetExecCtx(context.Background())
						execCtx.NumParallel = 1
						ctx := compute.SetExecCtx(context.Background(), execCtx)
						partitionBatch := partitionBatchByKey(ctx)
						b.ReportAllocs()
						b.ResetTimer()

						for b.Loop() {
							var seen int64
							for _, indices := range partitions {
								batch, err := partitionBatch(record, indices)
								if err != nil {
									b.Fatal(err)
								}
								seen += batch.NumRows()
								batch.Release()
							}
							partitionGatherBenchmarkRows = seen
						}
					})

					for _, groupSize := range []int{2, 4, 8, 16, 32, 64} {
						b.Run(fmt.Sprintf("group_%d", groupSize), func(b *testing.B) {
							partitionBatch := partitionBatchByKey(context.Background())
							b.ReportAllocs()
							b.ResetTimer()

							for b.Loop() {
								seen, err := benchmarkGroupedPartitionGather(partitionBatch, record, partitions, groupSize)
								if err != nil {
									b.Fatal(err)
								}
								partitionGatherBenchmarkRows = seen
							}
						})
					}
				})
			}
		})
		record.Release()
	}
}

func roundRobinPartitionRows(rows, partitionCount int) [][]int64 {
	partitions := make([][]int64, partitionCount)
	perPartition := (rows + partitionCount - 1) / partitionCount
	for i := range partitions {
		partitions[i] = make([]int64, 0, perPartition)
	}
	for row := range rows {
		partition := row % partitionCount
		partitions[partition] = append(partitions[partition], int64(row))
	}

	return partitions
}

func benchmarkGroupedPartitionGather(
	partitionBatch partitionBatchFn,
	record arrow.RecordBatch,
	partitions [][]int64,
	groupSize int,
) (int64, error) {
	var seen int64
	combined := make([]int64, 0, record.NumRows())
	offsets := make([]int64, groupSize+1)

	for groupStart := 0; groupStart < len(partitions); groupStart += groupSize {
		groupEnd := min(groupStart+groupSize, len(partitions))
		combined = combined[:0]
		offsets = offsets[:groupEnd-groupStart+1]
		offsets[0] = 0

		for i := groupStart; i < groupEnd; i++ {
			combined = append(combined, partitions[i]...)
			offsets[i-groupStart+1] = int64(len(combined))
		}

		grouped, err := partitionBatch(record, combined)
		if err != nil {
			return 0, err
		}

		for i := 0; i < groupEnd-groupStart; i++ {
			batch := grouped.NewSlice(offsets[i], offsets[i+1])
			seen += batch.NumRows()
			batch.Release()
		}
		grouped.Release()
	}

	return seen, nil
}

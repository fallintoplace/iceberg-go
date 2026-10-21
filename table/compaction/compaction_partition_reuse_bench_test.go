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

package compaction_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

var compactionPartitionBenchmarkSink int

func BenchmarkPlanCompactionPartitionBuckets(b *testing.B) {
	cases := []struct {
		name       string
		files      int
		partitions int
		grouped    bool
		skipEvery  int
	}{
		{"unpartitioned", 10_000, 1, true, 0},
		{"partition_local_64", 10_000, 64, true, 0},
		{"interleaved_64", 10_000, 64, false, 0},
		{"interleaved_1024", 10_000, 1024, false, 0},
		{"partition_local_64_mixed_candidates", 10_000, 64, true, 5},
		{"partition_local_64_large", 100_000, 64, true, 0},
		{"interleaved_64_large", 100_000, 64, false, 5},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			tasks := make([]table.FileScanTask, tc.files)
			wantSkipped := 0
			for i := range tasks {
				part := i % tc.partitions
				if tc.grouped {
					part = i * tc.partitions / tc.files
				}

				var partition map[int]any
				if tc.partitions != 1 {
					partition = map[int]any{
						1000: int32(part),
						1001: fmt.Sprintf("region-%02d", part%21),
						1002: iceberg.Date(part%365),
					}
				}

				sizeMB := int64(64)
				if tc.skipEvery > 0 && i%tc.skipEvery == 0 {
					sizeMB = 512
					wantSkipped++
				}
				file := newPartitionedDataFile(fmt.Sprintf("file-%d", i), sizeMB, partition)
				file.specID = int32(part % 3)
				tasks[i] = makeTask(file, 0, 0)
			}

			cfg := compaction.DefaultConfig()
			cfg.MinInputFiles = 1
			cfg.PackingLookback = 32

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				plan, err := cfg.PlanCompaction(tasks)
				if err != nil {
					b.Fatal(err)
				}
				if plan.TotalInputFiles != len(tasks) || plan.SkippedFiles != wantSkipped {
					b.Fatalf("unexpected plan counts: files=%d, skipped=%d", plan.TotalInputFiles, plan.SkippedFiles)
				}
				compactionPartitionBenchmarkSink = len(plan.Groups) + plan.SkippedFiles
			}
		})
	}
}

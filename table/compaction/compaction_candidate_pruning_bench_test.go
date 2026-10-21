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

	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
)

var compactionCandidatePlanBenchmarkSink compaction.Plan

// Measures the complete PlanCompaction call, including bin packing, for
// healthy-only, sparse-rewrite, mixed, and all-rewrite workloads.
func BenchmarkPlanCompactionCandidatePruning(b *testing.B) {
	const filesPerOp = 20_000
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     compaction.DefaultPackingLookback,
	}

	for _, tt := range []struct {
		name             string
		candidatePercent int
		partitionCount   int
	}{
		{name: "healthy", candidatePercent: 0, partitionCount: 128},
		{name: "sparse_5pct", candidatePercent: 5, partitionCount: 128},
		{name: "mixed_50pct", candidatePercent: 50, partitionCount: 128},
		{name: "all_candidates", candidatePercent: 100, partitionCount: 128},
		{name: "sparse_5pct_many_partitions", candidatePercent: 5, partitionCount: 4096},
	} {
		partitions := make([]map[int]any, tt.partitionCount)
		for i := range partitions {
			partitions[i] = map[int]any{
				1000: int32(i),
				1001: fmt.Sprintf("region-%d", i%17),
				1002: int64(i * 7),
				1003: int32(i % 11),
				1004: "category",
				1005: int64(i * 13),
				1006: int32(i % 5),
				1007: "country",
			}
		}

		tasks := make([]table.FileScanTask, filesPerOp)
		var totalInputBytes int64
		for i := range tasks {
			sizeMB := int64(100)
			if i*37%100 < tt.candidatePercent {
				sizeMB = 10
			}
			file := newPartitionedDataFile(fmt.Sprintf("file-%d.parquet", i), sizeMB, partitions[i%len(partitions)])
			totalInputBytes += file.FileSizeBytes()
			tasks[i] = makeTask(file, 0, 0)
		}

		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(filesPerOp, "files/op")
			b.ResetTimer()
			for b.Loop() {
				plan, err := cfg.PlanCompaction(tasks)
				if err != nil {
					b.Fatal(err)
				}
				if plan.TotalInputBytes != totalInputBytes || plan.TotalInputFiles != filesPerOp {
					b.Fatal("incorrect compaction input totals")
				}
				compactionCandidatePlanBenchmarkSink = plan
			}
		})
	}
}

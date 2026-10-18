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

var largeCompactionBenchmarkSink compaction.Plan

// BenchmarkPlanCompactionLargeLookback exercises the actual planning entry
// point with one partition and many undersized files. File sizes above half
// of the target force the large-window first-fit search to work hard.
func BenchmarkPlanCompactionLargeLookback(b *testing.B) {
	const mb = int64(1024 * 1024)
	for _, fileCount := range []int{10_000, 100_000} {
		for _, lookback := range []uint{128, 512} {
			b.Run(fmt.Sprintf("files=%d/lookback=%d", fileCount, lookback), func(b *testing.B) {
				cfg := compaction.DefaultConfig()
				cfg.TargetFileSizeBytes = 128 * mb
				cfg.MinFileSizeBytes = 96 * mb
				cfg.MaxFileSizeBytes = 230 * mb
				cfg.MinInputFiles = 1
				cfg.PackingLookback = lookback

				tasks := make([]table.FileScanTask, fileCount)
				for i := range tasks {
					// 65-95 MB files are eligible and cannot share a
					// 128 MB output bin with another such file.
					size := (65 + int64(i%31)) * mb
					tasks[i] = table.FileScanTask{
						File: &testDataFile{size: size, records: 100},
					}
				}

				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					plan, err := cfg.PlanCompaction(tasks)
					if err != nil {
						b.Fatal(err)
					}
					largeCompactionBenchmarkSink = plan
				}
			})
		}
	}
}

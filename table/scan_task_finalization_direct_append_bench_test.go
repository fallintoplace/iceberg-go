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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

// BenchmarkFinalizeTaskRangeSplitAppend isolates local Parquet task expansion.
// In particular, it measures the per-file temporary split slices that this
// optimization removes. The files and their offset metadata are built before
// timing so only planning work is included.
func BenchmarkFinalizeTaskRangeSplitAppend(b *testing.B) {
	const (
		fileSize   = int64(2048)
		targetSize = int64(128)
	)
	splitFile := finalizationTestDataFile(b, *iceberg.UnpartitionedSpec,
		"mem://benchmark/data/split.parquet", nil, fileSize,
		benchmarkFinalizationOffsets(31, 64))
	plainFile := finalizationTestDataFile(b, *iceberg.UnpartitionedSpec,
		"mem://benchmark/data/plain.parquet", nil, fileSize, nil)
	splits, ok := splitParquetScanTask(FileScanTask{File: splitFile, Length: fileSize}, targetSize)
	if !ok {
		b.Fatal("expected Parquet task to split")
	}

	for _, tc := range []struct {
		files, splitPercent int
	}{
		{4096, 0}, {4096, 25}, {4096, 100},
		{8192, 0}, {8192, 25}, {8192, 100},
	} {
		b.Run(fmt.Sprintf("files=%d/split=%d", tc.files, tc.splitPercent), func(b *testing.B) {
			tasks := make([]FileScanTask, tc.files)
			splitting := 0
			for i := range tasks {
				file := plainFile
				if i%100 < tc.splitPercent {
					file = splitFile
					splitting++
				}
				tasks[i] = FileScanTask{File: file, Length: fileSize}
			}
			want := len(tasks) + splitting*(len(splits)-1)
			scan := &Scan{}

			b.ReportAllocs()
			b.ReportMetric(float64(tc.files), "files/op")
			for b.Loop() {
				result := scan.finalizeTaskRange(tasks, 0, len(tasks), nil, targetSize)
				if result.err != nil || len(result.tasks) != want {
					b.Fatalf("finalizeTaskRange: got %d tasks, want %d, err=%v", len(result.tasks), want, result.err)
				}
				scanTaskFinalizationBenchmarkSink += len(result.tasks)
			}
		})
	}
}

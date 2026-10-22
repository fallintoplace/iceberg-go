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
	"github.com/stretchr/testify/require"
)

func TestFinalizeTaskRangeDirectSplitsMatchSingleFilePlanner(t *testing.T) {
	const fileSize int64 = 2048
	const splitSize int64 = 256

	makeFile := func(name string, size int64, offsets []int64) iceberg.DataFile {
		return finalizationTestDataFile(t, *iceberg.UnpartitionedSpec,
			"mem://finalization/"+name, nil, size, offsets)
	}

	firstRowID := int64(42)
	sequenceNum := int64(7)
	splitFile := makeFile("split.parquet", fileSize,
		[]int64{0, 256, 512, 768, 1024, 1280, 1536, 1792})
	deleteFile := makeFile("delete.parquet", 128, nil)
	tasks := []FileScanTask{
		{File: makeFile("prefix.parquet", fileSize, nil), Length: fileSize},
		{
			File: splitFile, Length: fileSize,
			DeleteFiles: []iceberg.DataFile{deleteFile},
			Residual: iceberg.AlwaysTrue{},
			FirstRowID: &firstRowID, DataSequenceNumber: &sequenceNum,
		},
		{File: makeFile("invalid.parquet", fileSize, []int64{0, 1024, 512}), Length: fileSize},
		{File: splitFile, Start: 256, Length: fileSize - 256},
		{File: makeFile("small.parquet", splitSize, []int64{0, 128}), Length: splitSize},
		{File: makeFile("sparse.parquet", fileSize, []int64{128, 640, 1152, 1664}), Length: fileSize},
		{File: makeFile("suffix.parquet", fileSize, nil), Length: fileSize},
	}

	for _, tc := range []struct {
		name       string
		start, end int
	}{
		{"all", 0, len(tasks)},
		{"unsplit-prefix", 0, 1},
		{"mixed-middle", 2, 6},
		{"split-only", 1, 2},
		{"suffix", 5, len(tasks)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var expected []FileScanTask
			var totalSize int64
			hasSplit := false
			for _, task := range tasks[tc.start:tc.end] {
				totalSize += task.File.FileSizeBytes()
				if splits, ok := splitParquetScanTask(task, splitSize); ok {
					expected = append(expected, splits...)
					hasSplit = true
				} else {
					expected = append(expected, task)
				}
			}

			result := (&Scan{}).finalizeTaskRange(tasks, tc.start, tc.end, nil, splitSize)
			require.NoError(t, result.err)
			require.Equal(t, hasSplit, result.expanded)
			require.Equal(t, expected, result.tasks)
			require.Equal(t, totalSize, result.metrics.totalFileSize)
		})
	}
}

var finalizeTaskRangeSplitBenchmarkSink int

// BenchmarkFinalizeTaskRangeSplitAppend isolates local finalization and
// measures splitting without manifest IO, residual binding, or fixture setup.
func BenchmarkFinalizeTaskRangeSplitAppend(b *testing.B) {
	const rowGroupSize int64 = 64
	const targetSize = 4 * rowGroupSize

	for _, tc := range []struct {
		files, groups, splitPercent int
	}{
		{1024, 32, 100},
		{1024, 32, 50},
		{1024, 32, 0},
		{8192, 32, 100},
	} {
		b.Run(fmt.Sprintf("files=%d/groups=%d/splittable=%d", tc.files, tc.groups, tc.splitPercent),
			func(b *testing.B) {
				offsets := make([]int64, tc.groups)
				for i := range offsets {
					offsets[i] = int64(i) * rowGroupSize
				}
				fileSize := int64(tc.groups) * rowGroupSize

				tasks := make([]FileScanTask, tc.files)
				for i := range tasks {
					splitOffsets := offsets
					if i%100 >= tc.splitPercent {
						splitOffsets = nil
					}
					file := finalizationTestDataFile(
						b, *iceberg.UnpartitionedSpec,
						fmt.Sprintf("mem://benchmark/local-finalization-%d.parquet", i),
						nil, fileSize, splitOffsets,
					)
					tasks[i] = FileScanTask{File: file, Length: fileSize}
				}

			scan := &Scan{}
			probe := scan.finalizeTaskRange(tasks, 0, len(tasks), nil, targetSize)
			require.NoError(b, probe.err)
			want := len(probe.tasks)

			b.ReportAllocs()
			b.ReportMetric(float64(tc.files), "files/op")
			b.ReportMetric(float64(want), "tasks/op")
			b.ResetTimer()
			for b.Loop() {
				result := scan.finalizeTaskRange(tasks, 0, len(tasks), nil, targetSize)
				if result.err != nil {
					b.Fatal(result.err)
				}
				if len(result.tasks) != want {
					b.Fatalf("got %d tasks, want %d", len(result.tasks), want)
				}
				finalizeTaskRangeSplitBenchmarkSink = len(result.tasks)
			}
		})
	}
}

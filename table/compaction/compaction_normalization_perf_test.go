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
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

func TestPlanCompactionSplitMetadataSharedAndCopiedDeletes(t *testing.T) {
	file := newDataFile("shared.parquet", 10)
	rowID, sequence := int64(17), int64(9)
	first := makeTask(file, 0, 0)
	first.DeleteFiles = []iceberg.DataFile{newDeleteFile("a.pos"), newDeleteFile("b.pos")}
	first.FirstRowID, first.DataSequenceNumber = &rowID, &sequence

	// The ordinary split path retains the same delete slice.
	shared := first
	shared.Start = file.size / 3
	// A remote or independently decoded task may have equal values but
	// different slice and pointer identities. It must still be accepted.
	copied := first
	copied.Start = 2 * file.size / 3
	copied.DeleteFiles = []iceberg.DataFile{newDeleteFile("a.pos"), newDeleteFile("b.pos")}
	copiedRowID, copiedSequence := rowID, sequence
	copied.FirstRowID, copied.DataSequenceNumber = &copiedRowID, &copiedSequence

	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 1
	plan, err := cfg.PlanCompaction([]table.FileScanTask{first, shared, copied})
	require.NoError(t, err)
	require.Equal(t, 1, plan.TotalInputFiles)
	require.Len(t, plan.Groups, 1)
	require.Len(t, plan.Groups[0].Tasks, 1)
	require.Equal(t, 2, plan.Groups[0].DeleteFileCount)

	// Matching paths are not enough: a separately decoded delete with different
	// content must still fail deep validation on the non-shared fallback.
	different := copied
	different.DeleteFiles = slices.Clone(copied.DeleteFiles)
	different.DeleteFiles[1] = newDeleteFile("other.pos")
	_, err = cfg.PlanCompaction([]table.FileScanTask{first, different})
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}

// BenchmarkPlanCompactionSplitNormalization measures the full planner with
// right-sized files. It excludes partition packing by configuring all files
// as non-candidates, so the workload focuses on split normalization and
// metadata consistency checks. Input construction is outside the timer.
func BenchmarkPlanCompactionSplitNormalization(b *testing.B) {
	for _, tc := range []struct {
		files         int
		splits        int
		deletes       int
		copiedDeletes bool
	}{
		{files: 20_000, splits: 4},
		{files: 5_000, splits: 8, deletes: 8},
		{files: 3_000, splits: 8, deletes: 64},
		{files: 5_000, splits: 8, deletes: 16, copiedDeletes: true},
	} {
		label := fmt.Sprintf("files=%d/splits=%d/deletes=%d/copied=%t", tc.files, tc.splits, tc.deletes, tc.copiedDeletes)
		b.Run(label, func(b *testing.B) {
			tasks := make([]table.FileScanTask, 0, tc.files*tc.splits)
			for i := range tc.files {
				file := newDataFile(fmt.Sprintf("file-%d.parquet", i), 500)
				deletes := make([]iceberg.DataFile, tc.deletes)
				for j := range deletes {
					deletes[j] = newDeleteFile(fmt.Sprintf("file-%d-delete-%d.pos", i, j))
				}
				original := table.FileScanTask{File: file, Length: file.size, DeleteFiles: deletes}
				for split := range tc.splits {
					task := original
					task.Start = int64(split) * file.size / int64(tc.splits)
					task.Length = file.size / int64(tc.splits)
					if split == tc.splits-1 {
						task.Length = file.size - task.Start
					}
					if tc.copiedDeletes {
						task.DeleteFiles = slices.Clone(deletes)
					}
					tasks = append(tasks, task)
				}
			}

			cfg := compaction.DefaultConfig()
			cfg.DeleteFileThreshold = 1_000
			cfg.DeleteRatioThreshold = 0

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				plan, err := cfg.PlanCompaction(tasks)
				if err != nil {
					b.Fatal(err)
				}
				compactionPlanBenchmarkSink = plan
			}
		})
	}
}

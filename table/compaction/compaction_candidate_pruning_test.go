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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// Ineligible files should not need their partition metadata to build the plan.
type unavailablePartitionDataFile struct{ iceberg.DataFile }

func (unavailablePartitionDataFile) Partition() map[int]any {
	panic("unneeded partition read for an ineligible file")
}

func (unavailablePartitionDataFile) SpecID() int32 {
	panic("unneeded spec id read for an ineligible file")
}

func TestPlanCompactionSkipsIneligiblePartitionMetadata(t *testing.T) {
	cfg := compaction.DefaultConfig()
	file := newDataFile("optimal.parquet", 500)
	task := makeTask(file, 0, 0)
	task.File = unavailablePartitionDataFile{DataFile: file}

	plan, err := cfg.PlanCompaction([]table.FileScanTask{task})
	require.NoError(t, err)
	require.Empty(t, plan.Groups)
	require.Equal(t, 1, plan.TotalInputFiles)
	require.Equal(t, file.FileSizeBytes(), plan.TotalInputBytes)
	require.Equal(t, 1, plan.SkippedFiles)
}

func TestPlanCompactionGroupsOnlyCandidates(t *testing.T) {
	cfg := compaction.Config{
		TargetFileSizeBytes: 100 * 1024 * 1024,
		MinFileSizeBytes:    75 * 1024 * 1024,
		MaxFileSizeBytes:    180 * 1024 * 1024,
		MinInputFiles:       2,
		DeleteFileThreshold: 5,
		PackingLookback:     compaction.DefaultPackingLookback,
	}

	a := map[int]any{1000: "alpha"}
	b := map[int]any{1000: "beta"}
	tasks := []table.FileScanTask{
		makeTask(newPartitionedDataFile("a-healthy", 100, a), 0, 0),
		makeTask(newPartitionedDataFile("b-healthy", 100, b), 0, 0),
		makeTask(newPartitionedDataFile("b-1", 10, b), 0, 0),
		makeTask(newPartitionedDataFile("a-1", 10, a), 0, 0),
		makeTask(newPartitionedDataFile("b-2", 10, b), 0, 0),
		makeTask(newPartitionedDataFile("a-2", 10, a), 0, 0),
		makeTask(newPartitionedDataFile("a-3", 10, a), 0, 0),
	}

	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)
	require.Equal(t, len(tasks), plan.TotalInputFiles)
	require.Equal(t, int64(250*1024*1024), plan.TotalInputBytes)
	require.Equal(t, 2, plan.SkippedFiles)

	got := make(map[string]int)
	for _, group := range plan.Groups {
		got[group.PartitionKey] += len(group.Tasks)
	}
	require.Equal(t, map[string]int{
		"0:1000=alpha;": 3,
		"0:1000=beta;":  2,
	}, got)
}

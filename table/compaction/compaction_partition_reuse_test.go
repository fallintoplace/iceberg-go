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

func TestPlanCompactionKeepsFirstSeenPartitionOrder(t *testing.T) {
	candidate := func(name string, specID int32, partition map[int]any) table.FileScanTask {
		file := newPartitionedDataFile(name, 64, partition)
		file.specID = specID

		return makeTask(file, 0, 0)
	}
	skipped := func(name string, specID int32, partition map[int]any) table.FileScanTask {
		file := newPartitionedDataFile(name, 512, partition)
		file.specID = specID

		return makeTask(file, 0, 0)
	}

	tasks := []table.FileScanTask{
		skipped("a-skipped", 0, map[int]any{1000: int32(1), 1001: "north"}),
		candidate("b1", 0, map[int]any{1000: int32(2), 1001: "south"}),
		candidate("a1", 0, map[int]any{1000: int32(1), 1001: "north"}),
		candidate("a2", 0, map[int]any{1001: "north", 1000: int32(1)}),
		candidate("a-spec1", 1, map[int]any{1000: int32(1), 1001: "north"}),
		candidate("b2", 0, map[int]any{1000: int32(2), 1001: "south"}),
		candidate("unpartitioned", 0, nil),
		skipped("b-skipped", 0, map[int]any{1000: int32(2), 1001: "south"}),
	}

	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 1
	plan, err := cfg.PlanCompaction(tasks)
	require.NoError(t, err)

	require.Equal(t, len(tasks), plan.TotalInputFiles)
	require.Equal(t, 2, plan.SkippedFiles)
	require.Len(t, plan.Groups, 4)

	wantKeys := []string{
		"0:1000=1;1001=north;",
		"0:1000=2;1001=south;",
		"1:1000=1;1001=north;",
		"0:_",
	}
	wantFiles := [][]string{
		{"a1", "a2"},
		{"b1", "b2"},
		{"a-spec1"},
		{"unpartitioned"},
	}
	for i, group := range plan.Groups {
		require.Equal(t, wantKeys[i], group.PartitionKey)
		gotFiles := make([]string, 0, len(group.Tasks))
		for _, task := range group.Tasks {
			gotFiles = append(gotFiles, task.File.FilePath())
			require.Equal(t, iceberg.ParquetFile, task.File.FileFormat())
		}
		require.Equal(t, wantFiles[i], gotFiles)
	}
}

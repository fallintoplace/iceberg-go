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
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/stretchr/testify/require"
)

// Compaction must never put different partition values in one rewrite group.
// The previous "id=value;" key could confuse delimiters, nulls, and strings.
func TestPlanCompactionSeparatesCollidingPartitions(t *testing.T) {
	cases := []struct {
		name  string
		left  map[int]any
		right map[int]any
	}{
		{
			name:  "embedded field delimiters",
			left:  map[int]any{1000: "a;1001=b", 1001: "c"},
			right: map[int]any{1000: "a", 1001: "b;1001=c"},
		},
		{
			name:  "null and literal string",
			left:  map[int]any{1000: nil},
			right: map[int]any{1000: "<nil>"},
		},
		{
			name:  "integer and numeric string",
			left:  map[int]any{1000: int32(42)},
			right: map[int]any{1000: "42"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := compaction.DefaultConfig()
			cfg.MinInputFiles = 2
			var tasks []table.FileScanTask
			for i := 0; i < 2; i++ {
				tasks = append(tasks,
					makeTask(newPartitionedDataFile(fmt.Sprintf("a-%d.parquet", i), 10, tc.left), 0, 0),
					makeTask(newPartitionedDataFile(fmt.Sprintf("b-%d.parquet", i), 10, tc.right), 0, 0),
				)
			}
			plan, err := cfg.PlanCompaction(tasks)
			require.NoError(t, err)
			require.Len(t, plan.Groups, 2)
			require.Equal(t, 0, plan.SkippedFiles)
			keys := make(map[string]struct{})
			for _, group := range plan.Groups {
				require.Len(t, group.Tasks, 2)
				keys[group.PartitionKey] = struct{}{}
				firstPrefix := group.Tasks[0].File.FilePath()[:2]
				for _, task := range group.Tasks {
					require.True(t, strings.HasPrefix(task.File.FilePath(), firstPrefix),
						"mixed partitions in compaction group %q", group.PartitionKey)
				}
			}
			require.Len(t, keys, 2)
		})
	}
}

func TestPlanCompactionRejectsUnsupportedPartitionValues(t *testing.T) {
	cfg := compaction.DefaultConfig()
	cfg.MinInputFiles = 1
	file := newPartitionedDataFile("bad.parquet", 10, map[int]any{1000: make(chan int)})
	plan, err := cfg.PlanCompaction([]table.FileScanTask{makeTask(file, 0, 0)})
	require.ErrorContains(t, err, "unsupported partition value type")
	require.ErrorContains(t, err, "bad.parquet")
	require.Empty(t, plan.Groups)
}

func TestCanonicalPartitionKeyNormalizesEquivalentIntegers(t *testing.T) {
	intKey, err := table.CanonicalPartitionKey(1, map[int]any{1000: int32(123)})
	require.NoError(t, err)
	dateKey, err := table.CanonicalPartitionKey(1, map[int]any{1000: iceberg.Date(123)})
	require.NoError(t, err)
	require.Equal(t, intKey, dateKey)

	stringKey, err := table.CanonicalPartitionKey(1, map[int]any{1000: "123"})
	require.NoError(t, err)
	require.NotEqual(t, intKey, stringKey)
}

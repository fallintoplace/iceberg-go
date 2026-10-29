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

func equalityDeleteGroupingTestFile(tb testing.TB, path string, fieldIDs []int) iceberg.DataFile {
	tb.Helper()
	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
		path, iceberg.ParquetFile, nil, nil, nil, 1, 128,
	)
	require.NoError(tb, err)
	return builder.EqualityFieldIDs(fieldIDs).Build()
}

func TestBuildEqualityDeleteSetsPerTaskSharesFileAndMergedSets(t *testing.T) {
	a := equalityDeleteGroupingTestFile(t, "a.parquet", []int{1})
	b := equalityDeleteGroupingTestFile(t, "b.parquet", []int{1})
	c := equalityDeleteGroupingTestFile(t, "c.parquet", []int{1})

	perFile := map[string]*equalityDeleteFileSet{
		a.FilePath(): newEqualityDeleteFileSet(0, &equalityDeleteSet{
			keys: set[string]{"a": {}}, fieldIDs: []int{1}, colNames: []string{"id"},
		}),
		b.FilePath(): newEqualityDeleteFileSet(1, &equalityDeleteSet{
			keys: set[string]{"b": {}}, fieldIDs: []int{1}, colNames: []string{"id"},
		}),
		c.FilePath(): newEqualityDeleteFileSet(2, &equalityDeleteSet{
			keys: set[string]{"c": {}}, fieldIDs: []int{1}, colNames: []string{"id"},
		}),
	}

	tasks := []FileScanTask{
		{EqualityDeleteFiles: []iceberg.DataFile{a}},
		{EqualityDeleteFiles: []iceberg.DataFile{a}},
		{EqualityDeleteFiles: []iceberg.DataFile{a, b}},
		{EqualityDeleteFiles: []iceberg.DataFile{b, a}},
		{EqualityDeleteFiles: []iceberg.DataFile{a, b, a}},
		{EqualityDeleteFiles: []iceberg.DataFile{a, c}},
	}
	got := buildEqualityDeleteSetsPerTask(tasks, perFile)
	require.Len(t, got, len(tasks))
	require.Same(t, perFile[a.FilePath()].equalityDeleteSet, got[0][0])
	require.Same(t, got[0][0], got[1][0])
	require.Same(t, got[2][0], got[3][0])
	require.Same(t, got[2][0], got[4][0])
	require.NotSame(t, got[2][0], got[5][0])
	require.Equal(t, set[string]{"a": {}, "b": {}}, got[2][0].keys)
	require.Equal(t, set[string]{"a": {}, "c": {}}, got[5][0].keys)

	// Merging must not write into a per-file map used by other tasks.
	got[2][0].keys["new"] = struct{}{}
	require.NotContains(t, perFile[a.FilePath()].keys, "new")
	require.NotContains(t, perFile[b.FilePath()].keys, "new")
}

func TestBuildEqualityDeleteSetsPerTaskSeparatesFieldsAndSkipsEmpty(t *testing.T) {
	a := equalityDeleteGroupingTestFile(t, "a.parquet", []int{1})
	b := equalityDeleteGroupingTestFile(t, "b.parquet", []int{1})
	c := equalityDeleteGroupingTestFile(t, "c.parquet", []int{2})
	empty := equalityDeleteGroupingTestFile(t, "empty.parquet", []int{3})
	missing := equalityDeleteGroupingTestFile(t, "missing.parquet", []int{4})

	perFile := map[string]*equalityDeleteFileSet{
		a.FilePath(): newEqualityDeleteFileSet(0, &equalityDeleteSet{
			keys: set[string]{"a": {}}, fieldIDs: []int{1}, colNames: []string{"id"},
		}),
		b.FilePath(): newEqualityDeleteFileSet(1, &equalityDeleteSet{
			keys: set[string]{"b": {}}, fieldIDs: []int{1}, colNames: []string{"id"},
		}),
		c.FilePath(): newEqualityDeleteFileSet(2, &equalityDeleteSet{
			keys: set[string]{"c": {}}, fieldIDs: []int{2}, colNames: []string{"category"},
		}),
		empty.FilePath(): newEqualityDeleteFileSet(3, &equalityDeleteSet{
			keys: set[string]{}, fieldIDs: []int{3}, colNames: []string{"empty"},
		}),
	}

	tasks := []FileScanTask{
		{EqualityDeleteFiles: []iceberg.DataFile{a, c, b}},
		{EqualityDeleteFiles: []iceberg.DataFile{b, a, c, empty, missing}},
		{EqualityDeleteFiles: []iceberg.DataFile{empty}},
		{EqualityDeleteFiles: []iceberg.DataFile{missing}},
	}
	got := buildEqualityDeleteSetsPerTask(tasks, perFile)
	require.Len(t, got, 2)

	for _, idx := range []int{0, 1} {
		require.Len(t, got[idx], 2)
		byField := make(map[int]*equalityDeleteSet)
		for _, deleteSet := range got[idx] {
			byField[deleteSet.fieldIDs[0]] = deleteSet
		}
		require.Equal(t, set[string]{"a": {}, "b": {}}, byField[1].keys)
		require.Equal(t, set[string]{"c": {}}, byField[2].keys)
	}
	require.NotContains(t, got, 2)
	require.NotContains(t, got, 3)
}

// benchmarkLegacyEqualityDeleteSets matches the previous per-task assembly.
// It is kept only in the benchmark to compare against the original algorithm.
func benchmarkLegacyEqualityDeleteSets(
	tasks []FileScanTask,
	perFile map[string]*equalityDeleteFileSet,
) map[int][]*equalityDeleteSet {
	perTask := make(map[int][]*equalityDeleteSet)
	for i, task := range tasks {
		if len(task.EqualityDeleteFiles) == 0 {
			continue
		}
		groups := make(map[string]*equalityDeleteSet)
		for _, file := range task.EqualityDeleteFiles {
			dk, ok := perFile[file.FilePath()]
			if !ok {
				continue
			}
			key := fmt.Sprint(dk.fieldIDs)
			group, exists := groups[key]
			if !exists {
				group = &equalityDeleteSet{
					keys: make(set[string]), fieldIDs: dk.fieldIDs, colNames: dk.colNames,
				}
				groups[key] = group
			}
			for value := range dk.keys {
				group.keys[value] = struct{}{}
			}
		}
		sets := make([]*equalityDeleteSet, 0, len(groups))
		for _, group := range groups {
			if len(group.keys) > 0 {
				sets = append(sets, group)
			}
		}
		if len(sets) > 0 {
			perTask[i] = sets
		}
	}
	return perTask
}

var equalityDeleteGroupingBenchmarkSink map[int][]*equalityDeleteSet

// BenchmarkBuildEqualityDeleteSetsPerTaskShared isolates per-task assembly
// from file reads. The old implementation copied every key for every task.
func BenchmarkBuildEqualityDeleteSetsPerTaskShared(b *testing.B) {
	for _, count := range []int{1, 2} {
		b.Run(fmt.Sprintf("files=%d", count), func(b *testing.B) {
			const taskCount = 128
			const keysPerFile = 8000

			perFile := make(map[string]*equalityDeleteFileSet, count)
			files := make([]iceberg.DataFile, 0, count)
			for fileIdx := 0; fileIdx < count; fileIdx++ {
				path := fmt.Sprintf("delete-%d.parquet", fileIdx)
				dataFile := equalityDeleteGroupingTestFile(b, path, []int{1})
				keys := make(set[string], keysPerFile)
				for idx := 0; idx < keysPerFile; idx++ {
					keys[fmt.Sprintf("%d-%08d", fileIdx, idx)] = struct{}{}
				}
				perFile[path] = newEqualityDeleteFileSet(fileIdx, &equalityDeleteSet{
					keys: keys, fieldIDs: []int{1}, colNames: []string{"id"},
				})
				files = append(files, dataFile)
			}

			tasks := make([]FileScanTask, taskCount)
			for idx := range tasks {
				tasks[idx].EqualityDeleteFiles = files
			}
			for _, impl := range []struct {
				name string
				run  func([]FileScanTask, map[string]*equalityDeleteFileSet) map[int][]*equalityDeleteSet
			}{
				{"before", benchmarkLegacyEqualityDeleteSets},
				{"after", buildEqualityDeleteSetsPerTask},
			} {
				b.Run(impl.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for idx := 0; idx < b.N; idx++ {
						equalityDeleteGroupingBenchmarkSink = impl.run(tasks, perFile)
					}
				})
			}
		})
	}
}

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
	"math/rand"
	"slices"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPositionalDeleteRangeIndexMatchesLinearReference(t *testing.T) {
	const (
		dataCount   = 120
		deleteCount = 1800
	)

	for _, seed := range []int64{1, 42, 73} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			deleteEntries := make([]iceberg.ManifestEntry, 0, deleteCount)
			for i := range deleteCount {
				start := rng.Intn(dataCount - 1)
				end := min(dataCount-1, start+1+rng.Intn(8))
				lower := positionalDeleteBenchmarkPath(start)
				upper := positionalDeleteBenchmarkPath(end)
				if i%11 == 0 {
					lower = "" // Unknown lower bound.
				}
				if i%13 == 0 {
					upper = "" // Unknown upper bound.
				}
				if i%67 == 0 {
					lower, upper = upper, lower // Malformed range, conservatively retained.
				}
				if i%17 == 0 {
					upper = positionalDeleteBenchmarkPath(dataCount - 1) // Overlapping ranges.
				}
				partition := map[int]any{1000: int32(i % 3)}
				var referenced *string
				if i%29 == 0 { // Existing path-scoped index must remain unaffected.
					path := positionalDeleteBenchmarkPath(start)
					referenced = &path
				}
				entry := newPositionalDeleteIndexTestEntryWithBounds(
					fmt.Sprintf("delete-%05d.parquet", i), 1, partition,
					int64(rng.Intn(10)-1), referenced, lower, upper)
				file := entry.DataFile().(*positionalDeleteIndexTestFile).mockDataFile
				if i%41 == 0 {
					file.count = 0
				}
				if i%43 == 0 {
					file.valueCounts = map[int]int64{filePathFieldID: 1}
					file.nullCounts = map[int]int64{filePathFieldID: 1}
				}
				deleteEntries = append(deleteEntries, entry)
			}

			idx, err := buildPositionalDeleteIndex(deleteEntries)
			require.NoError(t, err)
			for i := range dataCount {
				dataFile := newPositionalDeleteIndexDataEntry(
					positionalDeleteBenchmarkPath(i), 1,
					map[int]any{1000: int32(i % 3)}, int64(i%11-1))
				path := dataFile.DataFile().FilePath()
				key, err := canonicalPartitionKey(1, dataFilePartition(dataFile.DataFile()))
				require.NoError(t, err)
				want, err := appendPartitionDeletesFromSequence(nil,
					idx.byPartition[key], dataFile.SequenceNum(), path)
				require.NoError(t, err)
				want = appendPositionalDeletesFromSequence(want,
					idx.byPath[path], dataFile.SequenceNum())
				got, err := idx.forDataFile(dataFile)
				require.NoError(t, err)
				require.Equal(t, positionalDeletePaths(want), positionalDeletePaths(got),
					"data=%s sequence=%d", path, dataFile.SequenceNum())
			}
		})
	}
}

func TestPositionalDeleteRangeIndexConcurrentLookups(t *testing.T) {
	const deleteCount = 1500
	deletes := make([]iceberg.ManifestEntry, deleteCount)
	for i := range deletes {
		start := i % 300
		deletes[i] = newPositionalDeleteIndexTestEntryWithBounds(
			fmt.Sprintf("delete-%05d.parquet", i), 0, nil, int64(i%7), nil,
			positionalDeleteBenchmarkPath(start), positionalDeleteBenchmarkPath(start+3))
	}
	idx, err := buildPositionalDeleteIndex(deletes)
	require.NoError(t, err)
	key, err := canonicalPartitionKey(0, nil)
	require.NoError(t, err)
	rangeIndex := idx.rangeByPartition[key]
	require.NotNil(t, rangeIndex)
	require.Empty(t, rangeIndex.nodes) // Construction happens on first lookup.

	dataFile := newPositionalDeleteIndexDataEntry(positionalDeleteBenchmarkPath(77), 0, nil, 3)
	want, err := appendPartitionDeletesFromSequence(nil, idx.byPartition[key], 3, dataFile.DataFile().FilePath())
	require.NoError(t, err)
	wantPaths := positionalDeletePaths(want)

	var wg sync.WaitGroup
	errors := make(chan error, 16)
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 25 {
				got, lookupErr := idx.forDataFile(dataFile)
				if lookupErr != nil {
					errors <- lookupErr
					return
				}
				if !slices.Equal(positionalDeletePaths(got), wantPaths) {
					errors <- fmt.Errorf("different candidate ordering or contents")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	require.NotEmpty(t, rangeIndex.nodes)
}

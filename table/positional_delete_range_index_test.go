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
	"cmp"
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestPositionalDeletePathRangeIndexMatchesLinearSequenceScan(t *testing.T) {
	for _, tc := range []struct {
		name      string
		partition map[int]any
	}{
		{name: "integer partition", partition: map[int]any{1000: int32(7)}},
		{name: "multi-field partition", partition: map[int]any{1000: "eu", 1001: int32(7)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const deleteCount = 2048
			deletes := make([]iceberg.ManifestEntry, 0, deleteCount)
			for i := range deleteCount {
				from := (i * 37) % (deleteCount * 4)
				to := from + 6
				lower := fmt.Sprintf("s3://bucket/data-%08d.parquet", from)
				upper := fmt.Sprintf("s3://bucket/data-%08d.parquet", to)
				// Exercise missing and inverted bounds, skipped empty files,
				// repeated sequence numbers, and path-scoped entries.
				if i%89 == 0 {
					upper = ""
				}
				if i%127 == 0 {
					lower, upper = upper, lower
				}
				seq := int64((i*7)%17 - 1)
				var referenced *string
				if i%151 == 0 {
					path := fmt.Sprintf("s3://bucket/data-%08d.parquet", from)
					referenced = &path
				}
				entry := newPositionalDeleteIndexTestEntryWithBounds(
					fmt.Sprintf("delete-%05d.parquet", i), 1, tc.partition,
					seq, referenced, lower, upper)
				if i%173 == 0 {
					entry.DataFile().(*positionalDeleteIndexTestFile).count = 0
				}
				deletes = append(deletes, entry)
			}

			idx, err := buildPositionalDeleteIndex(deletes)
			require.NoError(t, err)
			var (
				partitionEntries []positionalPartitionDeleteEntry
				rangeIndex       *positionalDeletePathRangeIndex
			)
			if key, ok := singleIntegerPartitionKey(1, tc.partition); ok {
				partitionEntries = idx.byIntegerPartition[key]
				rangeIndex = idx.byIntegerPartitionRanges[key]
			} else {
				key, err := canonicalPartitionKey(1, tc.partition)
				require.NoError(t, err)
				partitionEntries = idx.byPartition[key]
				rangeIndex = idx.byPartitionRanges[key]
			}
			require.NotNil(t, rangeIndex, "sparse ranges should use the index")

			rng := rand.New(rand.NewSource(42))
			for query := range 600 {
				path := fmt.Sprintf("s3://bucket/data-%08d.parquet", rng.Intn(deleteCount*5))
				seq := int64(rng.Intn(24) - 3)
				dataEntry := newPositionalDeleteIndexDataEntry(path, 1, tc.partition, seq)
				got, err := idx.forDataFile(dataEntry)
				require.NoError(t, err)
				want, err := appendPartitionDeletesFromSequence(nil, partitionEntries, seq, path)
				require.NoError(t, err)
				want = appendPositionalDeletesFromSequence(want, idx.byPath[path], seq)
				require.Equalf(t, positionalDeletePaths(want), positionalDeletePaths(got),
					"query=%d path=%s seq=%d", query, path, seq)
			}
		})
	}
}

func TestPositionalDeletePathRangeIndexDenseFallback(t *testing.T) {
	const count = 512
	entries := make([]positionalPartitionDeleteEntry, count)
	for i := range entries {
		seq := int64(i % 17)
		entries[i] = newPositionalPartitionDeleteEntry(newPositionalDeleteIndexTestEntryWithBounds(
			fmt.Sprintf("delete-%d.parquet", i), 1, nil, seq, nil,
			"s3://bucket/data-00000000.parquet", "s3://bucket/data-99999999.parquet"))
	}
	require.Nil(t, buildPositionalDeletePathRangeIndex(entries))
}

func TestPositionalDeletePathRangeIndexPreservesEqualSequenceOrder(t *testing.T) {
	const count = 256
	entries := make([]positionalPartitionDeleteEntry, count)
	for i := range entries {
		seq := int64(i / 32)
		from := (i % 32) * 16
		entries[i] = newPositionalPartitionDeleteEntry(newPositionalDeleteIndexTestEntryWithBounds(
			fmt.Sprintf("delete-%d.parquet", i), 1, nil, seq, nil,
			fmt.Sprintf("data-%05d", from), fmt.Sprintf("data-%05d", from+18)))
	}
	index := buildPositionalDeletePathRangeIndex(entries)
	require.NotNil(t, index)
	for _, seq := range []int64{-1, 0, 3, 7, 9} {
		for path := 0; path < 520; path += 3 {
			value := fmt.Sprintf("data-%05d", path)
			got, err := appendPartitionDeletesFromRangeIndex(nil, entries, index, seq, value)
			require.NoError(t, err)
			want, err := appendPartitionDeletesFromSequence(nil, entries, seq, value)
			require.NoError(t, err)
			require.Equal(t, positionalDeletePaths(want), positionalDeletePaths(got))
		}
	}
}

var positionalDeletePathRangeSink []iceberg.DataFile

// BenchmarkPositionalDeletePathRangeIndex isolates partition-scoped matching.
// Index construction, manifest IO, and partition key encoding are excluded
// from lookup measurements and reported separately for fair attribution.
func BenchmarkPositionalDeletePathRangeIndex(b *testing.B) {
	for _, tc := range []struct {
		name  string
		count int
		dense bool
	}{
		{name: "sparse_2048", count: 2048},
		{name: "sparse_16384", count: 16384},
		{name: "dense_2048", count: 2048, dense: true},
		{name: "dense_16384", count: 16384, dense: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			entries := make([]positionalPartitionDeleteEntry, tc.count)
			for i := range entries {
				from, to := i*4, i*4+5
				if tc.dense {
					from, to = 0, tc.count*4
				}
				entries[i] = newPositionalPartitionDeleteEntry(newPositionalDeleteIndexTestEntryWithBounds(
					fmt.Sprintf("delete-%d.parquet", i), 0, nil, int64(i%16), nil,
					fmt.Sprintf("s3://bucket/data-%08d.parquet", from),
					fmt.Sprintf("s3://bucket/data-%08d.parquet", to)))
			}
			// The index builder receives sequence-sorted buckets in production.
			slices.SortStableFunc(entries, func(a, b positionalPartitionDeleteEntry) int {
				return cmp.Compare(a.entry.DataSequenceNumber(), b.entry.DataSequenceNumber())
			})
			index := buildPositionalDeletePathRangeIndex(entries)
			if tc.dense {
				if index != nil {
					b.Fatal("dense bucket should use linear fallback")
				}
			} else if index == nil {
				b.Fatal("sparse bucket should use range index")
			}

			paths := make([]string, 512)
			for i := range paths {
				paths[i] = fmt.Sprintf("s3://bucket/data-%08d.parquet", (i*31)%(tc.count*4))
			}
			for _, mode := range []string{"linear", "indexed"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					for i := range b.N {
						var err error
						if mode == "linear" {
							positionalDeletePathRangeSink, err = appendPartitionDeletesFromSequence(
								nil, entries, 5, paths[i%len(paths)])
						} else {
							positionalDeletePathRangeSink, err = appendPartitionDeletesFromRangeIndex(
								nil, entries, index, 5, paths[i%len(paths)])
						}
						if err != nil {
							b.Fatal(err)
						}
					}
				})
			}
			b.Run("build_index", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					_ = buildPositionalDeletePathRangeIndex(entries)
				}
			})
		})
	}
}

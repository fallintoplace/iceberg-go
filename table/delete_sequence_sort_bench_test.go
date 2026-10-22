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
)

// The payload approximates an index entry larger than a bare integer key.
// Data preparation and buffers are outside the timed region. Each iteration
// resets the input so the baseline never benefits from a prior sorted run.
type deleteSequenceSortBenchEntry struct {
	sequence int64
	ordinal  int64
	payload  [32]byte
}

var deleteSequenceSortBenchmarkSink []deleteSequenceSortBenchEntry

func deleteSequenceSortBenchmarkEntries(count int, order string) []deleteSequenceSortBenchEntry {
	rng := rand.New(rand.NewSource(1))
	entries := make([]deleteSequenceSortBenchEntry, count)
	for i := range entries {
		entries[i].ordinal = int64(i)
		switch order {
		case "ascending":
			entries[i].sequence = int64(i)
		case "descending":
			entries[i].sequence = int64(count - i)
		case "descending_ties":
			entries[i].sequence = int64((count - i) / 8)
		case "equal":
			entries[i].sequence = 7
		case "random":
			entries[i].sequence = int64(rng.Intn(max(1, count/4)))
		default:
			panic("unknown benchmark order: " + order)
		}
	}

	return entries
}

func BenchmarkDeleteSequenceSort(b *testing.B) {
	for _, count := range []int{1_000, 10_000, 100_000} {
		for _, order := range []string{"ascending", "descending", "descending_ties", "equal", "random"} {
			entries := deleteSequenceSortBenchmarkEntries(count, order)
			work := make([]deleteSequenceSortBenchEntry, count)
			for _, implementation := range []struct {
				name string
				sort func([]deleteSequenceSortBenchEntry)
			}{
				{"baseline", func(entries []deleteSequenceSortBenchEntry) {
					slices.SortStableFunc(entries, func(a, b deleteSequenceSortBenchEntry) int {
						return cmp.Compare(a.sequence, b.sequence)
					})
				}},
				{"optimized", func(entries []deleteSequenceSortBenchEntry) {
					if !sortMonotonicBySequence(entries, func(e deleteSequenceSortBenchEntry) int64 {
						return e.sequence
					}) {
						slices.SortStableFunc(entries, func(a, b deleteSequenceSortBenchEntry) int {
							return cmp.Compare(a.sequence, b.sequence)
						})
					}
				}},
			} {
				b.Run(fmt.Sprintf("%s/%d/%s", order, count, implementation.name), func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(count), "entries/op")
					b.ResetTimer()
					for b.Loop() {
						copy(work, entries)
						implementation.sort(work)
					}
					deleteSequenceSortBenchmarkSink = work
				})
			}
		}
	}
}

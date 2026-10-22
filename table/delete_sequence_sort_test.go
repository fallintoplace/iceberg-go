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
	"math"
	"math/rand"
	"slices"
	"testing"
)

type deleteSequenceSortTestEntry struct {
	sequence int64
	ordinal  int
}

// Compare against a stable sort, not merely the sequence-number result:
// equal-sequence entries must retain their original manifest order.
func TestStableSortBySequence(t *testing.T) {
	cases := []struct {
		name   string
		values []int64
	}{
		{"empty", nil},
		{"single", []int64{3}},
		{"ascending", []int64{-3, -1, 0, 3, 8}},
		{"ascending_ties", []int64{-3, -3, 0, 0, 2, 2}},
		{"descending", []int64{8, 4, 0, -5}},
		{"descending_ties", []int64{8, 8, 8, 4, 4, 0, 0, -3}},
		{"all_equal", []int64{4, 4, 4, 4}},
		{"unordered", []int64{3, 1, 2, 1, 3, 2, 1}},
		{"int64_extremes", []int64{math.MaxInt64, 0, 0, math.MinInt64}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := make([]deleteSequenceSortTestEntry, len(tc.values))
			for i, seq := range tc.values {
				got[i] = deleteSequenceSortTestEntry{sequence: seq, ordinal: i}
			}
			want := slices.Clone(got)
			slices.SortStableFunc(want, func(a, b deleteSequenceSortTestEntry) int {
				return cmp.Compare(a.sequence, b.sequence)
			})
			if !sortMonotonicBySequence(got, func(e deleteSequenceSortTestEntry) int64 {
				return e.sequence
			}) {
				slices.SortStableFunc(got, func(a, b deleteSequenceSortTestEntry) int {
					return cmp.Compare(a.sequence, b.sequence)
				})
			}
			if !slices.Equal(got, want) {
				t.Fatalf("stable order mismatch: got %#v, want %#v", got, want)
			}
		})
	}
}

// Randomized property coverage catches partial runs and mixed equal-key groups
// that could pass a sortedness check but break stable ordering on reversal.
func TestStableSortBySequenceRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, size := range []int{2, 3, 8, 31, 256, 4096} {
		for trial := 0; trial < 25; trial++ {
			input := make([]deleteSequenceSortTestEntry, size)
			for i := range input {
				input[i] = deleteSequenceSortTestEntry{
					sequence: int64(rng.Intn(max(1, size/8)) - size/16),
					ordinal:  i,
				}
			}
			if trial%3 == 1 {
				slices.SortFunc(input, func(a, b deleteSequenceSortTestEntry) int {
					return cmp.Compare(a.sequence, b.sequence)
				})
			} else if trial%3 == 2 {
				slices.SortFunc(input, func(a, b deleteSequenceSortTestEntry) int {
					return cmp.Compare(b.sequence, a.sequence)
				})
			}
			want := slices.Clone(input)
			got := slices.Clone(input)
			slices.SortStableFunc(want, func(a, b deleteSequenceSortTestEntry) int {
				return cmp.Compare(a.sequence, b.sequence)
			})
			if !sortMonotonicBySequence(got, func(e deleteSequenceSortTestEntry) int64 {
				return e.sequence
			}) {
				slices.SortStableFunc(got, func(a, b deleteSequenceSortTestEntry) int {
					return cmp.Compare(a.sequence, b.sequence)
				})
			}
			if !slices.Equal(got, want) {
				t.Fatalf("size=%d trial=%d: stable order mismatch", size, trial)
			}
		}
	}
}

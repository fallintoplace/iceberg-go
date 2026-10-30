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

package internal

import (
	"iter"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"strconv"
	"testing"
)

// This reference intentionally uses the original linear first-fit and
// largest-first algorithm. Different item IDs reveal any change to ordering
// even if many files have the same weight.
func linearLargestPacking[T any](input iter.Seq[T], target int64, lookback int, weightOf func(T) int64) iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		var bins []Bin[T]
		evict := func() []T {
			best := 0
			for i := 1; i < len(bins); i++ {
				if bins[i].Weight() > bins[best].Weight() {
					best = i
				}
			}
			out := bins[best].items
			bins = slices.Delete(bins, best, best+1)
			return out
		}
		for item := range input {
			weight := weightOf(item)
			first := -1
			for i := range bins {
				if bins[i].CanAdd(weight) {
					first = i
					break
				}
			}
			if first >= 0 {
				bins[first].Add(item, weight)
			} else {
				bin := Bin[T]{targetWeight: target}
				bin.Add(item, weight)
				bins = append(bins, bin)
				if len(bins) > lookback && !yield(evict()) {
					return
				}
			}
		}
		for len(bins) > 0 {
			if !yield(evict()) {
				return
			}
		}
	}
}

func TestIndexedLargestPackingMatchesLinear(t *testing.T) {
	for _, lookback := range []int{31, 32, 33, 64, 256, 1024} {
		for trial := range 20 {
			target := []int64{0, 1, 128, 1000, math.MaxInt64 / 2, math.MaxInt64}[trial%6]
			items := make([]int, 3*lookback+31)
			weights := make([]int64, len(items))
			rng := rand.New(rand.NewSource(int64(100 + trial)))
			for i := range items {
				items[i] = i
				switch trial % 5 {
				case 0:
					weights[i] = target/2 + 1
				case 1:
					weights[i] = int64(rng.Intn(129))
				case 2:
					weights[i] = target
				case 3:
					weights[i] = 0
				default:
					weights[i] = int64(rng.Intn(201) - 20)
				}
			}
			// Check fallback after the index has already been built.
			if trial%7 == 0 {
				weights[len(weights)/2] = -7
			} else if trial%7 == 1 {
				weights[len(weights)/2] = math.MaxInt64
			}
			weightOf := func(i int) int64 { return weights[i] }
			packer := SlicePacker[int]{TargetWeight: target, Lookback: lookback, LargestBinFirst: true}

			expected := slices.Collect(linearLargestPacking(slices.Values(items), target, lookback, weightOf))
			if got := packer.Pack(items, weightOf); !reflect.DeepEqual(got, expected) {
				t.Fatalf("Pack lookback=%d trial=%d mismatch", lookback, trial)
			}
			if got := slices.Collect(PackingIterator(slices.Values(items), target, lookback, weightOf, true)); !reflect.DeepEqual(got, expected) {
				t.Fatalf("PackingIterator lookback=%d trial=%d mismatch", lookback, trial)
			}
			reversed := slices.Clone(items)
			slices.Reverse(reversed)
			expectedEnd := slices.Collect(linearLargestPacking(slices.Values(reversed), target, lookback, weightOf))
			slices.Reverse(expectedEnd)
			for _, group := range expectedEnd {
				slices.Reverse(group)
			}
			if got := packer.PackEnd(items, weightOf); !reflect.DeepEqual(got, expectedEnd) {
				t.Fatalf("PackEnd lookback=%d trial=%d mismatch", lookback, trial)
			}
		}
	}
}

func TestIndexedLargestPackingStopsEarly(t *testing.T) {
	items := make([]int, 1000)
	for i := range items {
		items[i] = i
	}
	visited := 0
	input := func(yield func(int) bool) {
		for _, i := range items {
			visited++
			if !yield(i) {
				return
			}
		}
	}
	output := 0
	for range PackingIterator(input, 1, 256, func(int) int64 { return 1 }, true) {
		output++
		break
	}
	if output != 1 || visited >= len(items) {
		t.Fatalf("early stop consumed %d items, emitted %d groups", visited, output)
	}
}

var indexedLargestPackingBenchmarkSink [][]int64

func BenchmarkIndexedLargestPacking(b *testing.B) {
	for _, tc := range []struct {
		items, lookback int
		workload        string
	}{
		{4096, 1024, "large"},
		{16384, 4096, "large"},
		{32768, 8192, "large"},
		{32768, 8192, "easy"},
	} {
		b.Run("items="+strconv.Itoa(tc.items)+"/lookback="+strconv.Itoa(tc.lookback)+"/"+tc.workload, func(b *testing.B) {
			items := make([]int64, tc.items)
			for i := range items {
				if tc.workload == "large" {
					items[i] = 600 + int64(i%51)
				} else {
					items[i] = 1
				}
			}
			weightOf := func(value int64) int64 { return value }
			for _, mode := range []string{"linear", "indexed"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(tc.items), "items/op")
					b.ResetTimer()
					for range b.N {
						if mode == "linear" {
							indexedLargestPackingBenchmarkSink = slices.Collect(linearLargestPacking(slices.Values(items), 1000, tc.lookback, weightOf))
						} else {
							indexedLargestPackingBenchmarkSink = slices.Collect(PackingIterator(slices.Values(items), 1000, tc.lookback, weightOf, true))
						}
					}
				})
			}
		})
	}
}

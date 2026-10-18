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

package internal_test

import (
	"fmt"
	"iter"
	"math"
	"math/rand"
	"reflect"
	"slices"
	"testing"

	"github.com/apache/iceberg-go/internal"
)

// referenceFIFOPackEnd intentionally uses the previous linear first-fit
// algorithm, including signed addition and FIFO eviction semantics.
func referenceFIFOPackEnd(weights []int64, target int64, lookback int) [][]int {
	type bin struct {
		weight int64
		items  []int
	}
	var bins []bin
	var result [][]int
	for i := len(weights) - 1; i >= 0; i-- {
		w := weights[i]
		found := false
		for j := range bins {
			if bins[j].weight+w <= target {
				bins[j].weight += w
				bins[j].items = append(bins[j].items, i)
				found = true

				break
			}
		}
		if !found {
			bins = append(bins, bin{weight: w, items: []int{i}})
			if len(bins) > lookback {
				result = append(result, bins[0].items)
				bins = bins[1:]
			}
		}
	}

	for _, b := range bins {
		result = append(result, b.items)
	}
	slices.Reverse(result)
	for _, group := range result {
		slices.Reverse(group)
	}
	if result == nil {
		return make([][]int, 0)
	}

	return result
}

func TestPackingLargeLookbackMatchesLinear(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, lookback := range []int{127, 128, 129, 256, 512} {
		for _, count := range []int{0, 1, 128, 2047, 2048, 2049, 6000} {
			for _, workload := range []string{"no-fit", "mixed", "small", "zero", "negative", "oversized"} {
				weights := make([]int64, count)
				for i := range weights {
					switch workload {
					case "no-fit":
						weights[i] = 65 + int64(i%64)
					case "mixed":
						weights[i] = 1 + int64(rng.Intn(128))
					case "small":
						weights[i] = 1 + int64(i%8)
					case "zero":
						weights[i] = int64(i % 4)
					case "negative":
						weights[i] = int64(i%150) - 20
					case "oversized":
						weights[i] = 1 + int64(i%150)
					}
				}
				original := slices.Clone(weights)
				indices := make([]int, len(weights))
				for i := range indices {
					indices[i] = i
				}
				packer := internal.SlicePacker[int]{TargetWeight: 128, Lookback: lookback}
				got := packer.PackEnd(indices, func(i int) int64 { return weights[i] })
				want := referenceFIFOPackEnd(weights, 128, lookback)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("lookback=%d count=%d workload=%s: got %v, want %v",
						lookback, count, workload, got, want)
				}
				if !slices.Equal(weights, original) {
					t.Fatal("packing mutated the input weights")
				}
			}
		}
	}
}

// Exercise the path that starts with indexed packing, then must restore the
// linear bins when a negative or oversized item appears.
func TestPackingLargeLookbackFallbackAfterIndexing(t *testing.T) {
	for _, unusual := range []int64{-1, 150} {
		weights := make([]int64, 7000)
		indices := make([]int, len(weights))
		for i := range weights {
			indices[i] = i
			weights[i] = 80
			if i == 1500 {
				weights[i] = unusual
			}
		}
		packer := internal.SlicePacker[int]{TargetWeight: 128, Lookback: 128}
		got := packer.PackEnd(indices, func(i int) int64 { return weights[i] })
		want := referenceFIFOPackEnd(weights, 128, 128)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("unusual weight %d: indexed fallback changed packing", unusual)
		}
	}
}

func TestPackingLargeLookbackOverflowFallback(t *testing.T) {
	weights := []int64{math.MaxInt64, math.MaxInt64, -1, 0, 50, 99}
	indices := make([]int, len(weights))
	for i := range indices {
		indices[i] = i
	}
	packer := internal.SlicePacker[int]{TargetWeight: math.MaxInt64, Lookback: 128}
	got := packer.PackEnd(indices, func(i int) int64 { return weights[i] })
	want := referenceFIFOPackEnd(weights, math.MaxInt64, 128)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("overflow fallback: got %v, want %v", got, want)
	}
}

func TestPackingLargeLookbackEarlyStop(t *testing.T) {
	const lookback, stopAfter = 128, 3000
	consumed, weighted, emitted := 0, 0, 0
	source := iter.Seq[int](func(yield func(int) bool) {
		for i := range 10000 {
			consumed++
			if !yield(i) {
				return
			}
		}
	})
	packed := internal.PackingIterator(source, 128, lookback,
		func(int) int64 {
			weighted++
			return 80
		}, false)
	for group := range packed {
		if !slices.Equal(group, []int{emitted}) {
			t.Fatalf("group %d = %v", emitted, group)
		}
		emitted++
		if emitted == stopAfter {
			break
		}
	}
	if emitted != stopAfter || consumed != lookback+stopAfter || weighted != consumed {
		t.Fatalf("emitted=%d consumed=%d weighted=%d", emitted, consumed, weighted)
	}
}

var largeLookbackBenchmarkSink [][]int

func BenchmarkPackingLargeLookback(b *testing.B) {
	for _, count := range []int{100_000, 1_000_000} {
		for _, lookback := range []int{128, 512} {
			for _, workload := range []string{"no-fit", "mixed", "small"} {
				b.Run(fmt.Sprintf("items=%d/lookback=%d/%s", count, lookback, workload), func(b *testing.B) {
					weights := make([]int64, count)
					indices := make([]int, count)
					for i := range indices {
						indices[i] = i
						switch workload {
						case "no-fit":
							weights[i] = 65 + int64(i%64)
						case "mixed":
							weights[i] = 1 + int64((i*73)%128)
						case "small":
							weights[i] = 1 + int64(i%8)
						}
					}
					packer := internal.SlicePacker[int]{TargetWeight: 128, Lookback: lookback}
					weight := func(i int) int64 { return weights[i] }
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						largeLookbackBenchmarkSink = packer.PackEnd(indices, weight)
					}
				})
			}
		}
	}
}

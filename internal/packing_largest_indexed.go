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
	"slices"
)

// largestPackingWeightIndex tracks both first-fit eligibility and the largest
// open bin. Equal-weight bins are evicted in insertion order.
type largestPackingWeightIndex struct {
	size       int
	minWeights []int64
	maxWeights []int64
}

func newLargestPackingWeightIndex(capacity int) largestPackingWeightIndex {
	size := 1
	for size < capacity {
		size *= 2
	}
	idx := largestPackingWeightIndex{
		size:       size,
		minWeights: make([]int64, size*2),
		maxWeights: make([]int64, size*2),
	}
	idx.reset()
	return idx
}

func (idx *largestPackingWeightIndex) reset() {
	for i := range idx.minWeights {
		idx.minWeights[i] = math.MaxInt64
		idx.maxWeights[i] = math.MinInt64
	}
}

func (idx *largestPackingWeightIndex) update(slot int, weight int64, open bool) {
	node := idx.size + slot
	if open {
		idx.minWeights[node] = weight
		idx.maxWeights[node] = weight
	} else {
		idx.minWeights[node] = math.MaxInt64
		idx.maxWeights[node] = math.MinInt64
	}
	for node /= 2; node > 0; node /= 2 {
		left, right := node*2, node*2+1
		minWeight := min(idx.minWeights[left], idx.minWeights[right])
		maxWeight := max(idx.maxWeights[left], idx.maxWeights[right])
		if idx.minWeights[node] == minWeight && idx.maxWeights[node] == maxWeight {
			break
		}
		idx.minWeights[node] = minWeight
		idx.maxWeights[node] = maxWeight
	}
}

// firstFit finds the earliest active bin with weight <= maximum.
func (idx *largestPackingWeightIndex) firstFit(maximum int64) int {
	if idx.minWeights[1] > maximum {
		return -1
	}
	node := 1
	for node < idx.size {
		node *= 2
		if idx.minWeights[node] > maximum {
			node++
		}
	}
	return node - idx.size
}

// largest returns the earliest bin among those with maximum weight.
func (idx *largestPackingWeightIndex) largest() int {
	if idx.maxWeights[1] == math.MinInt64 {
		return -1
	}
	node := 1
	for node < idx.size {
		node *= 2
		if idx.maxWeights[node+1] > idx.maxWeights[node] {
			node++
		}
	}
	return node - idx.size
}

// indexedLargestPackingIterator preserves the linear algorithm for small or
// easy-to-pack inputs. After 128 active bins, a min/max segment tree makes
// first-fit lookup and largest-bin eviction O(log lookback). The index uses
// insertion-order slots; periodic compaction bounds space to O(lookback).
// If a weight falls outside [0, targetWeight], the original signed arithmetic
// is used for the rest of the input, including after index construction.
func indexedLargestPackingIterator[T any](
	input iter.Seq[T], targetWeight int64, lookback int, weightFunc func(T) int64,
) iter.Seq[[]T] {
	capacity := 2 * (lookback + 1)

	return func(yield func([]T) bool) {
		var linear []Bin[T]
		var bins []Bin[T]
		var active []bool
		var index largestPackingWeightIndex
		nextSlot, openBins := 0, 0
		indexed, indexSafe := false, true

		evictLinear := func() []T {
			largest := 0
			for i := 1; i < len(linear); i++ {
				if linear[i].Weight() > linear[largest].Weight() {
					largest = i
				}
			}
			out := linear[largest].items
			linear = slices.Delete(linear, largest, largest+1)
			return out
		}
		chooseIndexedLargest := func() int {
			if indexSafe {
				return index.largest()
			}
			largest := -1
			for i := 0; i < nextSlot; i++ {
				if active[i] && (largest < 0 || bins[i].Weight() > bins[largest].Weight()) {
					largest = i
				}
			}
			return largest
		}
		evictIndexed := func() []T {
			slot := chooseIndexedLargest()
			out := bins[slot].items
			bins[slot] = Bin[T]{}
			active[slot] = false
			openBins--
			if indexSafe {
				index.update(slot, 0, false)
			}
			return out
		}

		for item := range input {
			weight := weightFunc(item)
			if weight < 0 || weight > targetWeight {
				indexSafe = false
			}

			if !indexed {
				first := -1
				for i := range linear {
					if linear[i].CanAdd(weight) {
						first = i
						break
					}
				}
				if first >= 0 {
					linear[first].Add(item, weight)
				} else {
					bin := Bin[T]{targetWeight: targetWeight}
					bin.Add(item, weight)
					linear = append(linear, bin)
					if len(linear) > lookback && !yield(evictLinear()) {
						return
					}
				}

				if indexSafe && len(linear) == 128 {
					// Migrate only once the linear scan becomes expensive.
					bins = append(bins, linear...)
					active = make([]bool, len(bins))
					index = newLargestPackingWeightIndex(capacity)
					for i := range bins {
						active[i] = true
						index.update(i, bins[i].Weight(), true)
					}
					nextSlot, openBins = len(bins), len(bins)
					indexed = true
					linear = nil
				}
				continue
			}

			slot := -1
			if indexSafe {
				slot = index.firstFit(targetWeight - weight)
			} else {
				for i := 0; i < nextSlot; i++ {
					if active[i] && bins[i].CanAdd(weight) {
						slot = i
						break
					}
				}
			}
			if slot >= 0 {
				bins[slot].Add(item, weight)
				if indexSafe {
					index.update(slot, bins[slot].Weight(), true)
				}
				continue
			}

			if nextSlot == len(bins) && len(bins) < capacity {
				bins = append(bins, Bin[T]{})
				active = append(active, false)
			} else if nextSlot == capacity {
				// Preserve insertion order when reusing holes from evictions.
				kept := 0
				for i := 0; i < nextSlot; i++ {
					if active[i] {
						bins[kept] = bins[i]
						kept++
					}
				}
				clear(bins[kept:nextSlot])
				clear(active)
				for i := 0; i < kept; i++ {
					active[i] = true
				}
				nextSlot = kept
				if indexSafe {
					index.reset()
					for i := 0; i < kept; i++ {
						index.update(i, bins[i].Weight(), true)
					}
				}
			}
			bins[nextSlot] = Bin[T]{targetWeight: targetWeight}
			bins[nextSlot].Add(item, weight)
			active[nextSlot] = true
			if indexSafe {
				index.update(nextSlot, weight, true)
			}
			nextSlot++
			openBins++
			if openBins > lookback && !yield(evictIndexed()) {
				return
			}
		}

		if !indexed {
			for len(linear) > 0 {
				if !yield(evictLinear()) {
					return
				}
			}
		} else {
			for openBins > 0 {
				if !yield(evictIndexed()) {
					return
				}
			}
		}
	}
}

func shouldUseIndexedLargestPacking(lookback int, targetWeight int64) bool {
	return lookback >= 128 && lookback <= 16384 &&
		targetWeight >= 0 && targetWeight <= math.MaxInt64/2
}

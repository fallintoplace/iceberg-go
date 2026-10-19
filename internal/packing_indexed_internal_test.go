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
	"math"
	"math/rand"
	"strconv"
	"testing"
)

func TestPackingWeightIndexUpdateMaintainsFirstFit(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for _, capacity := range []int{1, 2, 31, 32, 33, 128, 513} {
		t.Run(strconv.Itoa(capacity), func(t *testing.T) {
			idx := newPackingWeightIndex(capacity)
			weights := make([]int64, capacity)
			for i := range weights {
				weights[i] = math.MaxInt64
			}

			for iteration := range 2_048 {
				bin := rng.Intn(capacity)
				weight := int64(rng.Intn(201))
				if iteration%17 == 0 {
					weight = math.MaxInt64
				}
				idx.update(bin, weight)
				weights[bin] = weight

				for range 4 {
					start := rng.Intn(capacity)
					end := start + 1 + rng.Intn(capacity-start)
					maxWeight := int64(rng.Intn(210))
					want := -1
					for i := start; i < end; i++ {
						if weights[i] <= maxWeight {
							want = i
							break
						}
					}
					if got := idx.firstFit(start, end, maxWeight); got != want {
						t.Fatalf("iteration %d firstFit(%d, %d, %d) = %d, want %d", iteration, start, end, maxWeight, got, want)
					}
				}
			}
		})
	}
}

func BenchmarkPackingWeightIndexUpdate(b *testing.B) {
	const capacity = 512
	idx := newPackingWeightIndex(capacity)
	for i := range capacity {
		idx.update(i, int64(i%128))
	}

	const operationCount = 4_096
	rng := rand.New(rand.NewSource(42))
	bins := make([]int, operationCount)
	weights := make([]int64, operationCount)
	for i := range bins {
		bins[i] = rng.Intn(capacity)
		weights[i] = int64(128 + rng.Intn(128))
	}

	operation := 0
	var generation int64
	b.ReportAllocs()
	for b.Loop() {
		idx.update(bins[operation], weights[operation]+generation)
		operation++
		if operation == operationCount {
			operation = 0
			generation = (generation + 1) & 31
		}
	}
}

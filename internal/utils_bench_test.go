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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go/internal"
)

type packEndBenchmarkItem struct {
	values [8]int64
}

var (
	packEndIntBenchmarkSink    [][]int64
	packEndStructBenchmarkSink [][]packEndBenchmarkItem
)

func BenchmarkSlicePackerPackEnd(b *testing.B) {
	for _, itemCount := range []int{128, 1_024, 16_384, 131_072} {
		b.Run("int64/items="+strconv.Itoa(itemCount), func(b *testing.B) {
			items := make([]int64, itemCount)
			packer := internal.SlicePacker[int64]{
				TargetWeight: 128,
				Lookback:     1,
			}

			b.ReportAllocs()
			b.ReportMetric(float64(itemCount), "items")
			b.ResetTimer()

			for range b.N {
				packEndIntBenchmarkSink = packer.PackEnd(items, func(int64) int64 {
					return 1
				})
			}
		})

		b.Run("struct/items="+strconv.Itoa(itemCount), func(b *testing.B) {
			items := make([]packEndBenchmarkItem, itemCount)
			packer := internal.SlicePacker[packEndBenchmarkItem]{
				TargetWeight: 128,
				Lookback:     1,
			}

			b.ReportAllocs()
			b.ReportMetric(float64(itemCount), "items")
			b.ResetTimer()

			for range b.N {
				packEndStructBenchmarkSink = packer.PackEnd(items, func(packEndBenchmarkItem) int64 {
					return 1
				})
			}
		})
	}
}

var largestBinFirstBenchmarkSink [][]int64

// BenchmarkSlicePackerLargestBinFirst measures largest-bin eviction with
// frequent new bins and with mixed weights that sometimes fit existing bins.
func BenchmarkSlicePackerLargestBinFirst(b *testing.B) {
	weight := func(v int64) int64 { return v }
	for _, lookback := range []int{16, 64, 256} {
		for _, workload := range []string{"no_fit", "mixed"} {
			items := make([]int64, 4096)
			for i := range items {
				if workload == "no_fit" {
					items[i] = 65 + int64(i%64)
				} else {
					items[i] = 1 + int64((i*73)%128)
				}
			}
			packer := internal.SlicePacker[int64]{
				TargetWeight:    128,
				Lookback:        lookback,
				LargestBinFirst: true,
			}

			b.Run("lookback="+strconv.Itoa(lookback)+"/"+workload, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					largestBinFirstBenchmarkSink = packer.Pack(items, weight)
				}
			})
		}
	}
}

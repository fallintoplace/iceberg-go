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

package dv

import "testing"

var keepMaskRangeDensitySink []byte

func BenchmarkKeepMaskForRangeDensity(b *testing.B) {
	const batchRows uint64 = 65_536
	const boundary uint64 = 1 << 32

	for _, tc := range []struct {
		name       string
		start      uint64
		length     uint64
		density    uint64
		outOfRange bool
	}{
		{name: "dense-start", length: batchRows, density: 1},
		{name: "half-start", length: batchRows, density: 2},
		{name: "one-in-sixteen", length: batchRows, density: 16},
		{name: "sparse-start", length: batchRows, density: 4096},
		{name: "dense-unaligned", start: 5, length: batchRows + 1, density: 1},
		{name: "dense-late", start: 4_000_005, length: batchRows, density: 1},
		{name: "dense-cross-bucket", start: boundary - 23, length: batchRows, density: 1},
		{name: "clean-batch", length: batchRows, density: 1, outOfRange: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bitmap := NewRoaringPositionBitmap()
			if tc.outOfRange {
				bitmap.Set(tc.length + 999)
			} else if tc.density == 1 {
				bitmap.SetRange(tc.start, tc.start+tc.length)
			} else {
				for i := uint64(0); i < tc.length; i += tc.density {
					bitmap.Set(tc.start + i)
				}
			}
			bitmap.RunLengthEncode()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				keepMaskRangeDensitySink = bitmap.KeepMaskForRange(tc.start, tc.start+tc.length)
			}
		})
	}
}

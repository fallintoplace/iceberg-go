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

// Exercise the dense word path with shifted reads, trailing bytes, and
// multiple 32-bit roaring buckets.
func TestKeepMaskForRangeDenseWordPath(t *testing.T) {
	const boundary uint64 = 1 << 32

	for _, tc := range []struct {
		name   string
		start  uint64
		length uint64
		first  uint64
		last   uint64
	}{
		{"aligned", 0, 65_536, 0, 65_536},
		{"unaligned", 5, 65_537, 0, 80_000},
		{"high bucket", boundary + 3, 65_536, boundary, boundary + 70_000},
		{"bucket crossing", boundary - 5, 65_536, boundary - 20, boundary + 70_000},
		{"short and shifted", 5, 13, 0, 28},
		{"partial trailing byte", 7, 129, 0, 160},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bitmap := NewRoaringPositionBitmap()
			bitmap.SetRange(tc.first, tc.last)
			bitmap.RunLengthEncode()
			assertKeepMaskForRange(t, bitmap, tc.start, tc.length)
		})
	}
}

// Sparse holes inside dense runs must remain set in the keep mask.
func TestKeepMaskForRangeDenseWithHoles(t *testing.T) {
	bitmap := NewRoaringPositionBitmap()
	bitmap.SetRange(0, 80_000)
	for position := uint32(3); position < 80_000; position += 17 {
		bitmap.bitmaps[0].Remove(position)
	}
	bitmap.RunLengthEncode()
	assertKeepMaskForRange(t, bitmap, 11, 65_537)
}

// A late batch should not allocate a dense prefix for a small range.
func TestKeepMaskForRangeDenseLateBatch(t *testing.T) {
	bitmap := NewRoaringPositionBitmap()
	bitmap.SetRange(4_000_011, 4_070_000)
	assertKeepMaskForRange(t, bitmap, 4_000_017, 65_537)
}

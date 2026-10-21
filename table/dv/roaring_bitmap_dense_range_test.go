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

import (
	"math/rand"
	"testing"
)

func TestKeepMaskForRangeDenseBulk(t *testing.T) {
	const boundary uint64 = 1 << 32

	t.Run("fully covered unaligned range", func(t *testing.T) {
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(3, 200_000)
		bitmap.RunLengthEncode()
		assertKeepMaskForRange(t, bitmap, 17, 65_537)
	})

	t.Run("near-full range with holes", func(t *testing.T) {
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(100, 200_000)
		for pos := uint32(101); pos < 100_000; pos += 1_997 {
			bitmap.bucket(0).Remove(pos)
		}
		bitmap.RunLengthEncode()
		assertKeepMaskForRange(t, bitmap, 99, 65_537)
	})

	t.Run("dense random deletions", func(t *testing.T) {
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(100, 200_000)
		r := rand.New(rand.NewSource(42))
		for pos := uint32(100); pos < 200_000; pos++ {
			if r.Intn(20) == 0 {
				bitmap.bucket(0).Remove(pos)
			}
		}
		bitmap.RunLengthEncode()
		assertKeepMaskForRange(t, bitmap, 103, 65_537)
	})

	t.Run("crosses 32-bit bucket boundary", func(t *testing.T) {
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(boundary-32_768, boundary+65_537)
		bitmap.bucket(0).Remove(uint32(boundary - 100))
		bitmap.bucket(1).Remove(100)
		bitmap.RunLengthEncode()
		assertKeepMaskForRange(t, bitmap, boundary-16_385, 65_539)
	})

	t.Run("dense high bucket with remote sparse entry", func(t *testing.T) {
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(7*boundary+100, 7*boundary+100_000)
		bitmap.Set(7*boundary + 3_000_000_000)
		bitmap.RunLengthEncode()
		assertKeepMaskForRange(t, bitmap, 7*boundary+103, 65_537)
	})

	t.Run("near 64-bit limit", func(t *testing.T) {
		const start = ^uint64(0) - 99_999
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(start, start+99_999)
		bitmap.RunLengthEncode()
		assertKeepMaskForRange(t, bitmap, start+9, 65_537)
	})

	t.Run("clean after dense area", func(t *testing.T) {
		bitmap := NewRoaringPositionBitmap()
		bitmap.SetRange(0, 65_536)
		assertKeepMaskForRange(t, bitmap, 65_536, 65_536)
	})
}

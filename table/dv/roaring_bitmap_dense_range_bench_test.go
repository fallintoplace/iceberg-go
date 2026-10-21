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
	"bytes"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var keepMaskDenseBenchmarkSink []byte

// legacyKeepMaskForRange is the pre-optimization implementation. Keeping it
// local to benchmarks makes old and new results reproducible on the same commit.
func legacyKeepMaskForRange(bitmap *RoaringPositionBitmap, start, end uint64) []byte {
	if start >= end {
		return nil
	}

	var out []byte
	for key, bm := range bitmap.bitmaps {
		base := uint64(key) << 32
		if end <= base {
			continue
		}

		lo := uint64(0)
		if start > base {
			lo = start - base
			if lo >= 1<<32 {
				continue
			}
		}
		hi := min(end-base, uint64(1)<<32)
		if lo >= hi || bm.CardinalityInRange(lo, hi) == 0 {
			continue
		}
		if out == nil {
			out = make([]byte, bitutil.BytesForBits(int64(end-start)))
			memory.Set(out, 0xFF)
		}

		it := bm.Iterator()
		it.AdvanceIfNeeded(uint32(lo))
		for it.HasNext() {
			pos := uint64(it.Next())
			if pos >= hi {
				break
			}
			offset := base + pos - start
			out[offset>>3] &^= byte(1 << (offset & 7))
		}
	}
	if out != nil {
		if pad := int64(len(out))*8 - int64(end-start); pad > 0 {
			bitutil.SetBitsTo(out, int64(end-start), pad, false)
		}
	}

	return out
}

// BenchmarkKeepMaskForRangeDense compares identical roaring bitmaps through
// the original per-position path and the bulk/range-aware production path.
func BenchmarkKeepMaskForRangeDense(b *testing.B) {
	const boundary uint64 = 1 << 32
	for _, tc := range []struct {
		name      string
		start     uint64
		length    uint64
		setup     func(*RoaringPositionBitmap)
	}{
		{
			name: "full_64k_unaligned", start: 103, length: 65_537,
			setup: func(bitmap *RoaringPositionBitmap) {
				bitmap.SetRange(100, 200_000)
			},
		},
		{
			name: "near_full_64k", start: 99, length: 65_537,
			setup: func(bitmap *RoaringPositionBitmap) {
				bitmap.SetRange(100, 200_000)
				for pos := uint32(101); pos < 100_000; pos += 1_997 {
					bitmap.bucket(0).Remove(pos)
				}
			},
		},
		{
			name: "full_256k", start: 999, length: 262_144,
			setup: func(bitmap *RoaringPositionBitmap) {
				bitmap.SetRange(0, 400_000)
			},
		},
		{
			name: "cross_bucket_64k", start: boundary-16_385, length: 65_539,
			setup: func(bitmap *RoaringPositionBitmap) {
				bitmap.SetRange(boundary-32_768, boundary+65_537)
			},
		},
		{
			name: "sparse_64k", start: 0, length: 65_536,
			setup: func(bitmap *RoaringPositionBitmap) {
				for pos := uint64(0); pos < 65_536; pos += 512 {
					bitmap.Set(pos)
				}
			},
		},
		{
			name: "alternating_64k", start: 0, length: 65_536,
			setup: func(bitmap *RoaringPositionBitmap) {
				for pos := uint64(0); pos < 65_536; pos += 2 {
					bitmap.Set(pos)
				}
			},
		},
		{
			name: "mostly_dense_64k", start: 0, length: 65_536,
			setup: func(bitmap *RoaringPositionBitmap) {
				bitmap.SetRange(0, 65_536)
				for pos := uint32(0); pos < 65_536; pos += 9 {
					bitmap.bucket(0).Remove(pos)
				}
			},
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			bitmap := NewRoaringPositionBitmap()
			tc.setup(bitmap)
			bitmap.RunLengthEncode()
			want := legacyKeepMaskForRange(bitmap, tc.start, tc.start+tc.length)
			got := bitmap.KeepMaskForRange(tc.start, tc.start+tc.length)
			if !bytes.Equal(got, want) || (got == nil) != (want == nil) {
				b.Fatalf("new mask differs from legacy for %s", tc.name)
			}
			for _, impl := range []struct {
				name string
				fn   func(*RoaringPositionBitmap, uint64, uint64) []byte
			}{
				{name: "legacy", fn: legacyKeepMaskForRange},
				{name: "bulk", fn: (*RoaringPositionBitmap).KeepMaskForRange},
			} {
				b.Run(impl.name, func(b *testing.B) {
					b.ReportAllocs()
					b.SetBytes(int64((tc.length + 7) / 8))
					b.ResetTimer()
					for b.Loop() {
						keepMaskDenseBenchmarkSink = impl.fn(bitmap, tc.start, tc.start+tc.length)
					}
				})
			}
		})
	}
}

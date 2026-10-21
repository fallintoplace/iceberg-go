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

package compaction

import (
	"math"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type compactionPartitionStringer int

func (compactionPartitionStringer) String() string { return "opaque" }

func TestSameCompactionPartition(t *testing.T) {
	id := uuid.MustParse("2baff9b1-4ea7-4a5c-a9c3-a611b7bfb900")
	tests := []struct {
		name  string
		left  map[int]any
		right map[int]any
		equal bool
	}{
		{"nil", nil, nil, true},
		{"empty", map[int]any{}, map[int]any{}, true},
		{"nil_and_empty", nil, map[int]any{}, true},
		{"different_map_same_values", map[int]any{10: int32(7), 20: "west"}, map[int]any{20: "west", 10: int32(7)}, true},
		{"different_ids", map[int]any{10: int32(7)}, map[int]any{11: int32(7)}, false},
		{"different_length", map[int]any{10: int32(7)}, map[int]any{10: int32(7), 11: nil}, false},
		{"different_values", map[int]any{10: int32(7)}, map[int]any{10: int32(8)}, false},
		{"different_types_same_print", map[int]any{10: int32(7)}, map[int]any{10: int64(7)}, false},
		{"nil_value", map[int]any{10: nil}, map[int]any{10: nil}, true},
		{"nil_vs_typed_nil", map[int]any{10: nil}, map[int]any{10: []byte(nil)}, false},
		{"boolean", map[int]any{10: true}, map[int]any{10: true}, true},
		{"integer", map[int]any{10: int(-7), 11: uint64(8)}, map[int]any{10: int(-7), 11: uint64(8)}, true},
		{"date", map[int]any{10: iceberg.Date(-4)}, map[int]any{10: iceberg.Date(-4)}, true},
		{"time", map[int]any{10: iceberg.Time(42)}, map[int]any{10: iceberg.Time(42)}, true},
		{"timestamp", map[int]any{10: iceberg.Timestamp(42)}, map[int]any{10: iceberg.Timestamp(42)}, true},
		{"timestamp_nano", map[int]any{10: iceberg.TimestampNano(42)}, map[int]any{10: iceberg.TimestampNano(42)}, true},
		{"uuid", map[int]any{10: id}, map[int]any{10: id}, true},
		{"binary_copy", map[int]any{10: []byte{0, 255, 7}}, map[int]any{10: []byte{0, 255, 7}}, true},
		{"binary_different", map[int]any{10: []byte{0, 255, 7}}, map[int]any{10: []byte{0, 255, 8}}, false},
		{"positive_zero", map[int]any{10: float64(0)}, map[int]any{10: float64(0)}, true},
		{"signed_zero", map[int]any{10: math.Copysign(0, -1)}, map[int]any{10: float64(0)}, false},
		{"float32_signed_zero", map[int]any{10: float32(math.Copysign(0, -1))}, map[int]any{10: float32(0)}, false},
		{"nan_same_bits", map[int]any{10: math.Float64frombits(0x7ff8000000000001)}, map[int]any{10: math.Float64frombits(0x7ff8000000000001)}, true},
		{"nan_different_bits", map[int]any{10: math.Float64frombits(0x7ff8000000000001)}, map[int]any{10: math.Float64frombits(0x7ff8000000000002)}, false},
		{"unknown_custom_type", map[int]any{10: compactionPartitionStringer(1)}, map[int]any{10: compactionPartitionStringer(1)}, false},
		{"unknown_struct", map[int]any{10: struct{ V int }{1}}, map[int]any{10: struct{ V int }{1}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sameCompactionPartition(tt.left, tt.right)
			if got != tt.equal {
				t.Fatalf("sameCompactionPartition(%v, %v) = %t, want %t", tt.left, tt.right, got, tt.equal)
			}
			if got {
				for _, specID := range []int32{-1, 0, 42} {
					leftKey := partitionBucketKey(specID, tt.left)
					rightKey := partitionBucketKey(specID, tt.right)
					if leftKey != rightKey {
						t.Fatalf("matched partitions produced different keys %q and %q", leftKey, rightKey)
					}
				}
			}
		})
	}
}

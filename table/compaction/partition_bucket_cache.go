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
	"bytes"
	"math"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// sameCompactionPartition checks whether two immutable partition tuples are
// guaranteed to produce the same partitionBucketKey suffix. Comparing common
// scalar values avoids sorting, formatting, and allocating a key per file in
// partition-local task streams.
//
// Return false for unfamiliar types, rather than risk merging buckets whose
// custom fmt.Stringer or %v representation might differ. Both callers then
// fall back to partitionBucketKey, retaining its existing key semantics.
// DataFile partition maps are borrowed, immutable values during planning.
func sameCompactionPartition(left, right map[int]any) bool {
	if len(left) != len(right) {
		return false
	}

	for id, value := range left {
		other, ok := right[id]
		if !ok {
			return false
		}

		switch x := value.(type) {
		case nil:
			if other != nil {
				return false
			}
		case bool:
			y, ok := other.(bool)
			if !ok || x != y {
				return false
			}
		case string:
			y, ok := other.(string)
			if !ok || x != y {
				return false
			}
		case int:
			y, ok := other.(int)
			if !ok || x != y {
				return false
			}
		case int32:
			y, ok := other.(int32)
			if !ok || x != y {
				return false
			}
		case int64:
			y, ok := other.(int64)
			if !ok || x != y {
				return false
			}
		case uint32:
			y, ok := other.(uint32)
			if !ok || x != y {
				return false
			}
		case uint64:
			y, ok := other.(uint64)
			if !ok || x != y {
				return false
			}
		case float32:
			y, ok := other.(float32)
			if !ok || math.Float32bits(x) != math.Float32bits(y) {
				return false
			}
		case float64:
			y, ok := other.(float64)
			if !ok || math.Float64bits(x) != math.Float64bits(y) {
				return false
			}
		case iceberg.Date:
			y, ok := other.(iceberg.Date)
			if !ok || x != y {
				return false
			}
		case iceberg.Time:
			y, ok := other.(iceberg.Time)
			if !ok || x != y {
				return false
			}
		case iceberg.Timestamp:
			y, ok := other.(iceberg.Timestamp)
			if !ok || x != y {
				return false
			}
		case iceberg.TimestampNano:
			y, ok := other.(iceberg.TimestampNano)
			if !ok || x != y {
				return false
			}
		case uuid.UUID:
			y, ok := other.(uuid.UUID)
			if !ok || x != y {
				return false
			}
		case []byte:
			y, ok := other.([]byte)
			if !ok || !bytes.Equal(x, y) {
				return false
			}
		default:
			return false
		}
	}

	return true
}

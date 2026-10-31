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

package table

import (
	"math"
	"reflect"
)

// Most files in a scan share one or two partition values. Retain only a
// small, worker-local set of residual results so large scans do not retain
// one entry per partition (or require synchronization between workers).
const maxCachedPartitionResiduals = 256

// A key contains the exact Go scalar values and types, not stringified or
// normalized values. This matters for malformed data: int32(1), int64(1),
// and a named integer type must not reuse one another's evaluation results.
// Only scalar values are accepted because slices, maps and mutable objects
// cannot be used safely as map keys.
type partitionResidualCacheKey struct {
	specID   int
	fields   uint8
	firstID  int
	secondID int
	first    any
	second   any
}

func residualCacheableScalar(value any) bool {
	if value == nil {
		return true
	}

	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32,
		reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.String:
		return true
	case reflect.Float32, reflect.Float64:
		// NaN is not equal to itself, so it cannot produce a cache hit.
		return !math.IsNaN(v.Float())
	default:
		return false
	}
}

// partitionResidualKey supports empty, one-field, and two-field partitions.
// Iterating a Go map has no stable order, so sort the two fields by ID.
// Larger and non-scalar partitions use the ordinary evaluator, unchanged.
func partitionResidualKey(specID int, partition map[int]any) (partitionResidualCacheKey, bool) {
	key := partitionResidualCacheKey{specID: specID}
	if len(partition) > 2 {
		return key, false
	}

	for fieldID, value := range partition {
		if !residualCacheableScalar(value) {
			return key, false
		}
		if key.fields == 0 {
			key.firstID, key.first = fieldID, value
		} else {
			key.secondID, key.second = fieldID, value
			if key.secondID < key.firstID {
				key.firstID, key.secondID = key.secondID, key.firstID
				key.first, key.second = key.second, key.first
			}
		}
		key.fields++
	}

	return key, true
}

type cachedPartitionResidual[T any] struct {
	value   T
	changed bool
}

// partitionResidualCache is owned by one finalization worker. Errors are
// deliberately never cached: each malformed file must retain its own error
// context. The last-key shortcut handles long manifest-order partition runs
// without hashing, while the bounded map helps interleaved partitions.
type partitionResidualCache[T any] struct {
	entries   map[partitionResidualCacheKey]cachedPartitionResidual[T]
	lastKey       partitionResidualCacheKey
	lastValue     cachedPartitionResidual[T]
	hasLast       bool
	lookups       uint8
	hits          uint8
	lowHitWindows uint8
	disabled      bool
}

// lookup tracks hit density over successive 64-key windows. Disable the
// cache after two miss-heavy windows; a high-cardinality stream otherwise
// pays for key hashing and cache churn without reusing any residual results.
func (c *partitionResidualCache[T]) lookup(
	key partitionResidualCacheKey,
) (cachedPartitionResidual[T], bool) {
	if c.disabled {
		return cachedPartitionResidual[T]{}, false
	}

	var value cachedPartitionResidual[T]
	var ok bool
	if c.hasLast && c.lastKey == key {
		value, ok = c.lastValue, true
	} else if value, ok = c.entries[key]; ok {
		c.lastKey, c.lastValue, c.hasLast = key, value, true
	}

	c.lookups++
	if ok {
		c.hits++
	}
	if c.lookups == 64 {
		if c.hits < 8 {
			c.lowHitWindows++
		} else {
			c.lowHitWindows = 0
		}
		c.lookups, c.hits = 0, 0
		if c.lowHitWindows == 2 {
			c.disabled = true
			c.entries = nil
			c.hasLast = false
		}
	}

	return value, ok
}

func (c *partitionResidualCache[T]) remember(
	key partitionResidualCacheKey, value T, changed bool,
) {
	if c.disabled {
		return
	}
	if c.entries == nil {
		c.entries = make(map[partitionResidualCacheKey]cachedPartitionResidual[T], 16)
	}
	if len(c.entries) == maxCachedPartitionResiduals {
		clear(c.entries)
	}
	cached := cachedPartitionResidual[T]{value: value, changed: changed}
	c.entries[key] = cached
	c.lastKey, c.lastValue, c.hasLast = key, cached, true
}

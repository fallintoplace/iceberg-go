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
	"testing"
)

type residualCacheTestDay int32

func TestPartitionResidualKeyOrderingAndTypes(t *testing.T) {
	first := map[int]any{1002: "eu", 1000: int32(7)}
	second := map[int]any{1000: int32(7), 1002: "eu"}
	expected, ok := partitionResidualKey(3, first)
	if !ok {
		t.Fatal("two scalar partition fields should be cacheable")
	}
	for i := 0; i < 100; i++ {
		got, ok := partitionResidualKey(3, second)
		if !ok || got != expected {
			t.Fatalf("partition map order changed key: got %+v, expected %+v", got, expected)
		}
	}
	if expected.fields != 2 || expected.firstID != 1000 || expected.secondID != 1002 {
		t.Fatalf("fields not sorted by ID: %+v", expected)
	}

	tests := []struct {
		name      string
		specID    int
		partition map[int]any
	}{
		{"different spec", 4, first},
		{"different field", 3, map[int]any{1001: int32(7), 1002: "eu"}},
		{"different value", 3, map[int]any{1000: int32(8), 1002: "eu"}},
		{"different type", 3, map[int]any{1000: int64(7), 1002: "eu"}},
		{"named integer", 3, map[int]any{1000: residualCacheTestDay(7), 1002: "eu"}},
		{"missing field", 3, map[int]any{1000: int32(7)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := partitionResidualKey(tt.specID, tt.partition)
			if !ok || got == expected {
				t.Fatalf("distinct partitions should produce distinct keys: %+v", got)
			}
		})
	}

	if a, ok := partitionResidualKey(3, map[int]any{1000: residualCacheTestDay(7)}); !ok {
		t.Fatal("named integer scalar should be supported")
	} else if b, ok := partitionResidualKey(3, map[int]any{1000: int32(7)}); !ok || a == b {
		t.Fatal("named integer and underlying int32 must not collide")
	}
}

func TestPartitionResidualKeyUnsupportedValues(t *testing.T) {
	for _, partition := range []map[int]any{
		{1: []byte("mutable")},
		{1: map[string]int{"a": 1}},
		{1: new(int)},
		{1: math.NaN()},
		{1: 1, 2: 2, 3: 3},
	} {
		if key, ok := partitionResidualKey(0, partition); ok {
			t.Fatalf("unsafe partition unexpectedly cacheable: %+v", key)
		}
	}
	for _, partition := range []map[int]any{
		nil,
		{},
		{1: nil},
		{1: true},
		{1: float32(2.5)},
		{1: "one"},
		{1: residualCacheTestDay(5)},
	} {
		if _, ok := partitionResidualKey(0, partition); !ok {
			t.Fatalf("scalar partition unexpectedly uncacheable: %+v", partition)
		}
	}
}

func TestPartitionResidualCacheBoundedAndStable(t *testing.T) {
	var cache partitionResidualCache[string]
	const spec = 2
	first, ok := partitionResidualKey(spec, map[int]any{5: int32(7)})
	if !ok {
		t.Fatal("expected cacheable key")
	}
	if _, hit := cache.lookup(first); hit {
		t.Fatal("empty cache returned a hit")
	}
	cache.remember(first, "simplified", true)
	if value, hit := cache.lookup(first); !hit || value.value != "simplified" || !value.changed {
		t.Fatalf("cached result lost: %+v, hit=%v", value, hit)
	}

	// Force several complete cache generations. Resident entries must remain
	// bounded rather than scaling with the number of data files.
	for i := 0; i < 20_000; i++ {
		key, ok := partitionResidualKey(spec, map[int]any{5: i + 1000})
		if !ok {
			t.Fatal("integer partition should be cacheable")
		}
		cache.remember(key, "not simplified", false)
		if len(cache.entries) > maxCachedPartitionResiduals {
			t.Fatalf("cache grew beyond limit: %d", len(cache.entries))
		}
		if got, hit := cache.lookup(key); !hit || got.changed || got.value != "not simplified" {
			t.Fatalf("incorrect cache result for %d: %+v hit=%v", i, got, hit)
		}
	}
	if _, hit := cache.lookup(first); hit {
		t.Fatal("evicted key should not reappear")
	}
}

func TestPartitionResidualCacheMatchesDirectEvaluation(t *testing.T) {
	// Check both adjacent and interleaved values, two-field maps, and keys
	// that are deliberately excluded from caching.
	partitions := []map[int]any{
		{1: int32(7)}, {1: int32(8)}, {1: int32(7)},
		{1: int64(7)}, {1: residualCacheTestDay(7)},
		{1: "west", 2: int32(1)}, {2: int32(1), 1: "west"},
		{1: []byte("not cacheable")}, {1: nil}, {1: nil},
		{1: math.NaN()}, {1: math.NaN()},
	}
	evaluate := func(p map[int]any) (string, bool) {
		value, ok := p[1]
		if !ok || value == nil {
			return "null", false
		}
		switch v := value.(type) {
		case int32:
			return "int32", v%2 == 0
		case int64:
			return "int64", v%2 == 0
		case residualCacheTestDay:
			return "named", v%2 == 0
		case string:
			return v, true
		default:
			return "opaque", false
		}
	}
	var cache partitionResidualCache[string]
	for repeat := 0; repeat < 10; repeat++ {
		for _, partition := range partitions {
			want, wantChanged := evaluate(partition)
			key, supported := partitionResidualKey(2, partition)
			var got string
			var changed, hit bool
			if supported {
				if value, ok := cache.lookup(key); ok {
					got, changed, hit = value.value, value.changed, true
				}
			}
			if !hit {
				got, changed = evaluate(partition)
				if supported {
					cache.remember(key, got, changed)
				}
			}
			if got != want || changed != wantChanged {
				t.Fatalf("cached evaluation (%q, %v) != direct (%q, %v)",
					got, changed, want, wantChanged)
			}
		}
	}
}

func TestPartitionResidualCacheDisablesUnproductiveLookup(t *testing.T) {
	var cache partitionResidualCache[int]
	for i := 0; i < 128; i++ {
		key, ok := partitionResidualKey(1, map[int]any{1000: int32(i)})
		if !ok {
			t.Fatal("integer partition unexpectedly uncacheable")
		}
		if _, hit := cache.lookup(key); hit {
			t.Fatalf("unexpected hit for unique partition %d", i)
		}
		cache.remember(key, i, true)
	}
	if !cache.disabled {
		t.Fatal("cache should stop hashing after sustained misses")
	}
	if len(cache.entries) != 0 {
		t.Fatal("disabled cache still retains entries")
	}
	key, _ := partitionResidualKey(1, map[int]any{1000: int32(0)})
	if _, hit := cache.lookup(key); hit {
		t.Fatal("disabled cache unexpectedly returned a hit")
	}
	cache.remember(key, 1, true)
	if len(cache.entries) != 0 {
		t.Fatal("disabled cache should not retain new entries")
	}
}

func TestPartitionResidualCacheKeepsInterleavedHits(t *testing.T) {
	var cache partitionResidualCache[int]
	// First 64 keys are unique, but the second window repeats them. A
	// one-window miss check would disable the cache before those hits.
	for i := 0; i < 128; i++ {
		value := int32(i % 64)
		key, _ := partitionResidualKey(1, map[int]any{1000: value})
		if _, hit := cache.lookup(key); !hit {
			cache.remember(key, int(value), true)
		}
	}
	if cache.disabled {
		t.Fatal("cache should stay enabled for recurring partitions")
	}
	key, _ := partitionResidualKey(1, map[int]any{1000: int32(17)})
	if got, hit := cache.lookup(key); !hit || got.value != 17 {
		t.Fatalf("repeated partition not cached: %+v, hit=%v", got, hit)
	}
}

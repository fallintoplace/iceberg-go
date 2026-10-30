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
	"slices"
	"testing"
)

func TestPartitionRecordIndexMatchesLinearGrouping(t *testing.T) {
	positiveZero := float64(0)
	negativeZero := math.Copysign(0, -1)
	records := []partitionRecord{
		{}, {},
		{nil}, {nil},
		{[]byte(nil)}, {[]byte{}},
		{[]byte("x")}, {[]byte("x")}, {"x"},
		{float32(math.NaN())}, {float32(math.NaN())},
		{math.NaN()}, {math.NaN()},
		{positiveZero}, {negativeZero}, {positiveZero},
		{int32(7)}, {int64(7)},
		{int32(7), []byte("a")},
		{int32(7), []byte("b")},
		{int32(7), []byte("a")},
		{int32(7), nil},
		{int32(7), []byte(nil)},
		{int32(7), []byte{}},
		{"a", "b"}, {"a", "c"}, {"a", "b"},
	}
	var index partitionRecordIndex
	var unique []partitionRecord
	for i, key := range records {
		next := len(unique)
		got := index.getOrInsert(key, next)
		want := slices.IndexFunc(unique, func(existing partitionRecord) bool {
			return partitionRecordsEqual(existing, key)
		})
		if want < 0 {
			want = next
			unique = append(unique, key)
		}
		if got != want {
			t.Fatalf("record %d (%v): got group %d, want %d", i, key, got, want)
		}
	}
}

func TestPartitionRecordIndexCopiesBinaryKeys(t *testing.T) {
	value := []byte("original")
	var index partitionRecordIndex
	if got := index.getOrInsert(partitionRecord{value}, 0); got != 0 {
		t.Fatalf("first index: got %d, want 0", got)
	}
	value[0] = 'X'
	if got := index.getOrInsert(partitionRecord{[]byte("original")}, 1); got != 0 {
		t.Fatalf("original binary partition was not retained: got %d, want 0", got)
	}
	if got := index.getOrInsert(partitionRecord{value}, 1); got != 1 {
		t.Fatalf("mutated binary partition reused original index: got %d, want 1", got)
	}
}

func TestPartitionRecordIndexManyPartitions(t *testing.T) {
	const unique = 2048
	var index partitionRecordIndex
	for i := range unique {
		key := partitionRecord{int32(i % 17), int64(i)}
		if got := index.getOrInsert(key, i); got != i {
			t.Fatalf("new key %d: got %d", i, got)
		}
	}
	for i := unique - 1; i >= 0; i-- {
		key := partitionRecord{int32(i % 17), int64(i)}
		if got := index.getOrInsert(key, unique); got != i {
			t.Fatalf("existing key %d: got %d", i, got)
		}
	}
}

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

	"github.com/apache/iceberg-go"
)

func TestInspectPartitionAggregateIndexSingleFieldMatchesTree(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "partition", Type: iceberg.PrimitiveTypes.Int32},
	}}
	index := newInspectPartitionAggregateIndex(partitionType)
	baseline := newInspectPartitionAggregateTree()
	if !index.singleField {
		t.Fatal("one-field partition should use the flat index")
	}

	// Keep the key types distinct, including named/unnamed floats, null vs
	// binary nil, and []byte vs string values with identical contents.
	values := []any{
		nil, int32(7), int64(7), "7", []byte("7"), []byte(nil), []byte{},
		true, math.Copysign(0, -1), float64(0), math.Float64frombits(0x7ff8000000000001),
		float32(1), math.Float32frombits(0x7fc00001),
	}
	for _, value := range values {
		aggregate := &inspectPartitionAggregate{}
		record := partitionRecord{value}
		baseline.insert(record, aggregate)
		index.insert(record, aggregate)
	}

	for _, value := range append(values,
		math.Float64frombits(0x7ff8000000000002),
		math.Float32frombits(0x7fc00002),
		[]byte("missing"), int32(8),
	) {
		partition := map[int]any{1000: value}
		want := baseline.lookupPartition(partition, partitionType)
		if got := index.lookupPartition(partition, partitionType); got != want {
			t.Fatalf("lookup for %T(%v) = %p, want %p", value, value, got, want)
		}
	}
	if got := index.lookupPartition(nil, partitionType); got != baseline.lookupPartition(nil, partitionType) {
		t.Fatalf("missing field lookup = %p, want %p", got, baseline.lookupPartition(nil, partitionType))
	}
}

func TestInspectPartitionAggregateIndexBinaryInputIsNotRetained(t *testing.T) {
	partitionType := &iceberg.StructType{FieldList: []iceberg.NestedField{
		{ID: 1000, Name: "partition", Type: iceberg.PrimitiveTypes.Binary},
	}}
	index := newInspectPartitionAggregateIndex(partitionType)
	original := []byte("reused buffer")
	aggregate := &inspectPartitionAggregate{}
	index.insert(partitionRecord{original}, aggregate)
	original[0] = 'X'

	if got := index.lookupPartition(map[int]any{1000: []byte("reused buffer")}, partitionType); got != aggregate {
		t.Fatalf("lookup after input mutation = %p, want %p", got, aggregate)
	}
	if got := index.lookupPartition(map[int]any{1000: original}, partitionType); got != nil {
		t.Fatalf("mutated input unexpectedly matched aggregate %p", got)
	}
}

func TestInspectPartitionAggregateIndexMultiFieldAndEmptyFallback(t *testing.T) {
	for _, fields := range [][]iceberg.NestedField{
		nil,
		{
			{ID: 1000, Name: "first", Type: iceberg.PrimitiveTypes.Binary},
			{ID: 1001, Name: "second", Type: iceberg.PrimitiveTypes.Int32},
		},
	} {
		partitionType := &iceberg.StructType{FieldList: fields}
		index := newInspectPartitionAggregateIndex(partitionType)
		if index.singleField || index.tree == nil {
			t.Fatal("non-single-field partition should use the existing tree")
		}

		record := make(partitionRecord, len(fields))
		partition := make(map[int]any, len(fields))
		for i, field := range fields {
			var value any = int32(9)
			if i == 0 {
				value = []byte("key")
			}
			record[i] = value
			partition[field.ID] = value
		}
		aggregate := &inspectPartitionAggregate{}
		index.insert(record, aggregate)
		if got := index.lookupPartition(partition, partitionType); got != aggregate {
			t.Fatalf("fields=%d: got %p, want %p", len(fields), got, aggregate)
		}
	}
}

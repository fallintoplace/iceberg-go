// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may obtain a copy of the License at
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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var inspectPartitionIndexBenchmarkSink int

func BenchmarkInspectPartitionAggregateIndex(b *testing.B) {
	for _, tc := range []struct {
		name       string
		fields     int
		partitions int
		binary     bool
	}{
		{name: "int32-100-partitions", fields: 1, partitions: 100},
		{name: "int32-100k-partitions", fields: 1, partitions: 100_000},
		{name: "binary-100-partitions", fields: 1, partitions: 100, binary: true},
		{name: "binary-100k-partitions", fields: 1, partitions: 100_000, binary: true},
		{name: "multi-int32-8-fields", fields: 8, partitions: 100},
	} {
		b.Run(tc.name, func(b *testing.B) {
			const fileCount = 100_000
			fields := make([]iceberg.NestedField, tc.fields)
			for i := range fields {
				fields[i] = iceberg.NestedField{
					ID: 1000 + i, Name: fmt.Sprintf("partition_%d", i),
					Type: iceberg.PrimitiveTypes.Int32,
				}
			}
			partitionType := &iceberg.StructType{FieldList: fields}
			partitions := make([]map[int]any, fileCount)
			records := make([]partitionRecord, fileCount)
			for i := range fileCount {
				partitionID := i % tc.partitions
				partition := make(map[int]any, tc.fields)
				record := make(partitionRecord, tc.fields)
				for j, field := range fields {
					var value any = int32(partitionID + j)
					if tc.binary {
						value = []byte(fmt.Sprintf("partition-%d-field-%d", partitionID, j))
					}
					partition[field.ID] = value
					record[j] = value
				}
				partitions[i] = partition
				records[i] = record
			}

			b.Run("tree", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(fileCount, "files/op")
				for b.Loop() {
					index := newInspectPartitionAggregateTree()
					unique := 0
					for i, partition := range partitions {
						aggregate := index.lookupPartition(partition, partitionType)
						if aggregate == nil {
							aggregate = &inspectPartitionAggregate{}
							index.insert(records[i], aggregate)
							unique++
						}
						aggregate.dataFileCount++
					}
					inspectPartitionIndexBenchmarkSink = unique
				}
			})
			b.Run("indexed", func(b *testing.B) {
				b.ReportAllocs()
				b.ReportMetric(fileCount, "files/op")
				for b.Loop() {
					index := newInspectPartitionAggregateIndex(partitionType)
					unique := 0
					for i, partition := range partitions {
						aggregate := index.lookupPartition(partition, partitionType)
						if aggregate == nil {
							aggregate = &inspectPartitionAggregate{}
							index.insert(records[i], aggregate)
							unique++
						}
						aggregate.dataFileCount++
					}
					inspectPartitionIndexBenchmarkSink = unique
				}
			})
		})
	}
}

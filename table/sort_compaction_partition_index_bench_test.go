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
	"fmt"
	"slices"
	"strconv"
	"testing"
)

var sortCompactionPartitionBenchmarkSink int

// BenchmarkSortCompactionPartitionGrouping isolates the partition lookup in
// both sort-compaction paths. Linear retains the previous implementation.
func BenchmarkSortCompactionPartitionGrouping(b *testing.B) {
	for _, tc := range []struct {
		partitions int
		fields     int
		repeated   bool
	}{
		{partitions: 128, fields: 1},
		{partitions: 1024, fields: 1},
		{partitions: 4096, fields: 1},
		{partitions: 1024, fields: 2},
		{partitions: 4096, fields: 2},
		{partitions: 4096, fields: 1, repeated: true},
		{partitions: 4096, fields: 2, repeated: true},
	} {
		name := "Distinct"
		if tc.repeated {
			name = "Repeated64"
		}

		records := make([]partitionRecord, tc.partitions)
		for i := range records {
			key := i
			if tc.repeated {
				key %= 64
			}
			if tc.fields == 1 {
				records[i] = partitionRecord{int32(key)}
			} else {
				records[i] = partitionRecord{int32(key), []byte("grp-" + strconv.Itoa(key))}
			}
		}

		name = fmt.Sprintf("%s/%d/Fields%d", name, tc.partitions, tc.fields)
		b.Run(name+"/Linear", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var groups []partitionRecord
				for _, key := range records {
					if slices.IndexFunc(groups, func(existing partitionRecord) bool {
						return partitionRecordsEqual(existing, key)
					}) < 0 {
						groups = append(groups, key)
					}
				}
				sortCompactionPartitionBenchmarkSink = len(groups)
			}
		})
		b.Run(name+"/Indexed", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var index partitionRecordIndex
				groups := 0
				for _, key := range records {
					if index.getOrInsert(key, groups) == groups {
						groups++
					}
				}
				sortCompactionPartitionBenchmarkSink = groups
			}
		})
	}
}

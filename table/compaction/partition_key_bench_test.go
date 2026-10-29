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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
)

var compactionPartitionKeyBenchmarkSink string

// BenchmarkCompactionPartitionKey compares the legacy formatter and the
// collision-safe encoder used by PlanCompaction. Data setup is not timed.
func BenchmarkCompactionPartitionKey(b *testing.B) {
	cases := []struct {
		name string
		part map[int]any
	}{
		{"single_string", map[int]any{1000: "2026-10-29"}},
		{"single_int32", map[int]any{1000: int32(12345)}},
		{"single_int64", map[int]any{1000: int64(1234567890123)}},
		{"single_null", map[int]any{1000: nil}},
		{"eight_mixed", map[int]any{1000: "2026", 1001: int32(23), 1002: int64(34343),
			1003: true, 1004: "berlin", 1005: nil, 1006: 3.14, 1007: "y-z"}},
		{"sixteen_strings", make(map[int]any, 16)},
	}
	for i := 0; i < 16; i++ {
		cases[len(cases)-1].part[1000+i] = strings.Repeat(strconv.Itoa(i), 5)
	}
	for _, tc := range cases {
		b.Run(tc.name+"/before", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				compactionPartitionKeyBenchmarkSink = partitionBucketKey(3, tc.part)
			}
		})
		b.Run(tc.name+"/after", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				key, err := table.CanonicalPartitionKey(3, tc.part)
				if err != nil {
					b.Fatal(err)
				}
				compactionPartitionKeyBenchmarkSink = key
			}
		})
	}
}

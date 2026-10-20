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

import "testing"

var computeHashBenchmarkSink string

func BenchmarkComputeHash(b *testing.B) {
	const dataFileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	b.ReportAllocs()
	for range b.N {
		computeHashBenchmarkSink = computeHash(dataFileName)
	}
}

var joinLocationPathBenchmarkSink string

func BenchmarkJoinLocationPath(b *testing.B) {
	cases := []struct {
		name     string
		location string
		elem     []string
	}{
		{name: "metadata_1part", location: "s3://warehouse/db/table/metadata", elem: []string{"00025-f32a1111.metadata.json"}},
		{name: "object_store_2parts", location: "s3://warehouse/db/table/data", elem: []string{"0101/0110/1001/10110010", "part-00001-8a32e.snappy.parquet"}},
		{name: "three_parts", location: "s3://warehouse/db/table/data", elem: []string{"p=1", "q=blue", "part-00001.parquet"}},
		{name: "trailing_2parts", location: "s3://warehouse/db/table/data///", elem: []string{"0101/0110/1001/10110010", "part-00001-8a32e.snappy.parquet"}},
		{name: "zero_parts", location: "s3://warehouse/db/table/data"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				joinLocationPathBenchmarkSink = joinLocationPath(tc.location, tc.elem...)
			}
		})
	}
}

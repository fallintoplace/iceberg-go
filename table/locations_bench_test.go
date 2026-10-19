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
	const base = "s3://bucket/tenant/table/data///"
	for _, tc := range []struct {
		name  string
		parts []string
	}{
		{name: "single", parts: []string{"part-00001.parquet"}},
		{name: "object_store", parts: []string{"0000/1001/0010/11110100", "part-00001.parquet"}},
		{name: "multiple", parts: []string{"a", "b", "c", "part-00001.parquet"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				joinLocationPathBenchmarkSink = joinLocationPath(base, tc.parts...)
			}
		})
	}
}

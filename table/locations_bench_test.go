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

var locationBenchmarkSink string

func BenchmarkJoinLocationPath(b *testing.B) {
	const base = "s3://iceberg-warehouse/data/my_table/xyz"
	const hash = "0101/0110/1001/10110010"
	const fileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	for _, tt := range []struct {
		name  string
		elems []string
	}{
		{"one", []string{fileName}},
		{"two", []string{hash, fileName}},
		{"three", []string{hash, "partition=x", fileName}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				locationBenchmarkSink = joinLocationPath(base, tt.elems...)
			}
		})
	}
}

func BenchmarkObjectStoreNewDataLocation(b *testing.B) {
	const fileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	for _, tt := range []struct {
		name           string
		partitionPaths bool
	}{
		{"flat", false},
		{"partitioned", true},
	} {
		b.Run(tt.name, func(b *testing.B) {
			provider := &objectStoreLocationProvider{
				simpleLocationProvider: &simpleLocationProvider{
					dataPath: "s3://iceberg-warehouse/data/my_table/xyz",
				},
				includePartitionPaths: tt.partitionPaths,
			}

			b.ReportAllocs()
			for range b.N {
				locationBenchmarkSink = provider.NewDataLocation(fileName)
			}
		})
	}
}

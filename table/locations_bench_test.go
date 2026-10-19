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
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
)

var computeHashBenchmarkSink string

func BenchmarkComputeHash(b *testing.B) {
	const dataFileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	b.ReportAllocs()
	for range b.N {
		computeHashBenchmarkSink = computeHash(dataFileName)
	}
}

var joinLocationBenchmarkSink string

func BenchmarkJoinLocationPath(b *testing.B) {
	for _, tc := range []struct {
		name, location string
		parts          []string
	}{
		{"metadata-one-component", "s3://bucket/warehouse/db/table/metadata", []string{"00001-a.metadata.json"}},
		{"object-store-two-components", "s3://bucket/warehouse/db/table/data", []string{"0011/0101/1010/11100000", "part-0001-0123456789abcdef-c000.snappy.parquet"}},
		{"three-components", "s3://bucket/warehouse/db/table/data", []string{"0011", "0101", "part-0001.snappy.parquet"}},
	} {
		b.Run(tc.name+"/before", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				joinLocationBenchmarkSink = strings.TrimRight(tc.location, "/") + "/" + strings.Join(tc.parts, "/")
			}
		})
		b.Run(tc.name+"/after", func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				joinLocationBenchmarkSink = joinLocationPath(tc.location, tc.parts...)
			}
		})
	}
}

func BenchmarkObjectStoreNewDataLocation(b *testing.B) {
	const fileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	for _, partitionPaths := range []string{"false", "true"} {
		b.Run("partition-paths="+partitionPaths, func(b *testing.B) {
			provider := newObjectStoreLocationProvider("s3://bucket/warehouse/db/table",
				iceberg.Properties{WriteObjectStorePartitionedPathsKey: partitionPaths})

			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				joinLocationBenchmarkSink = provider.NewDataLocation(fileName)
			}
		})
	}
}

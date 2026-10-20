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
	"path"
	"testing"
)

var (
	computeHashBenchmarkSink         [hashPathLength]byte
	objectStoreLocationBenchmarkSink string
)

func BenchmarkComputeHash(b *testing.B) {
	const dataFileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"

	b.ReportAllocs()
	for range b.N {
		computeHashBenchmarkSink = computeHash(dataFileName)
	}
}

// priorObjectStoreDataLocation retains the old formatting for comparison.
func priorObjectStoreDataLocation(p *objectStoreLocationProvider, name string) string {
	if path.Dir(name) != "." {
		return p.simpleLocationProvider.NewDataLocation(name)
	}

	hash := computeHash(name)
	if p.includePartitionPaths {
		return joinLocationPath(p.dataPath, string(hash[:]), name)
	}

	return joinLocationPath(p.dataPath, string(hash[:])+"-"+name)
}

func TestObjectStoreDataLocationMatchesPriorFormatting(t *testing.T) {
	for _, dataPath := range []string{"", "/", "s3://bucket/table/data", "s3://bucket/table/data///"} {
		for _, includePartitionPaths := range []bool{false, true} {
			provider := &objectStoreLocationProvider{
				simpleLocationProvider: &simpleLocationProvider{dataPath: dataPath},
				includePartitionPaths: includePartitionPaths,
			}
			for _, name := range []string{"a", "part-00164929.parquet", "nested/file.parquet", "file #%.parquet"} {
				got := provider.NewDataLocation(name)
				want := priorObjectStoreDataLocation(provider, name)
				if got != want {
					t.Fatalf("location for data path %q, file %q, partition paths %t: got %q, want %q",
						dataPath, name, includePartitionPaths, got, want)
				}
			}
		}
	}
}

func BenchmarkObjectStoreDataLocation(b *testing.B) {
	const fileName = "part-00000-8f4c2e1d-2d6b-4e38-9f7f-2bca3d8e6a10-c000.snappy.parquet"
	for _, includePartitionPaths := range []bool{false, true} {
		provider := &objectStoreLocationProvider{
			simpleLocationProvider: &simpleLocationProvider{dataPath: "s3://bucket/table/data"},
			includePartitionPaths: includePartitionPaths,
		}
		b.Run(fmt.Sprintf("partition-paths=%t", includePartitionPaths), func(b *testing.B) {
			b.Run("before", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					objectStoreLocationBenchmarkSink = priorObjectStoreDataLocation(provider, fileName)
				}
			})
			b.Run("after", func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					objectStoreLocationBenchmarkSink = provider.NewDataLocation(fileName)
				}
			})
		})
	}
}

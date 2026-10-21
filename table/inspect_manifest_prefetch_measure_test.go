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
	"testing"
)

// Temporary CI-only calibration. Removed after the benchmark results are
// captured in the PR description.
func TestInspectManifestPrefetchMeasurement(t *testing.T) {
	for _, manifests := range []int{1, 8, 32} {
		for _, concurrency := range []int{1, inspectManifestPrefetchConcurrency} {
			t.Run(fmt.Sprintf("manifests=%d/concurrency=%d", manifests, concurrency), func(t *testing.T) {
				result := testing.Benchmark(func(b *testing.B) {
					tbl := benchmarkInspectPartitionTable(b, manifests)
					inspect := tbl.Inspect()
					inspect.manifestPrefetchConcurrency = concurrency
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						reader, err := inspect.Files(b.Context())
						if err != nil {
							b.Fatal(err)
						}
						var rows int64
						for reader.Next() {
							rows += reader.RecordBatch().NumRows()
						}
						readErr := reader.Err()
						reader.Release()
						if readErr != nil {
							b.Fatal(readErr)
						}
						if rows != int64(manifests) {
							b.Fatalf("got %d rows, want %d", rows, manifests)
						}
					}
				})
				t.Logf("ICEBERG_PREFETCH_BENCH manifests=%d concurrency=%d ns/op=%d B/op=%d allocs/op=%d N=%d",
					manifests, concurrency, result.NsPerOp(), result.AllocedBytesPerOp(), result.AllocsPerOp(), result.N)
			})
		}
	}
}

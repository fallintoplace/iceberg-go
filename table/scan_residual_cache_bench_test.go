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

	"github.com/apache/iceberg-go"
)

var scanResidualCacheBenchSink iceberg.BooleanExpression

// BenchmarkScanPartitionResidualCache compares the previous per-file
// evaluator with the bounded cache using the real Iceberg partition residual
// evaluator. It intentionally excludes manifest I/O, delete planning, and
// Parquet splitting, so a speedup here is not an end-to-end scan speedup.
// Each operation starts a fresh per-worker cache, as finalization does.
func BenchmarkScanPartitionResidualCache(b *testing.B) {
	schema := simpleSchema()
	spec := partitionedSpec()
	expr := iceberg.NewAnd(
		iceberg.GreaterThan(iceberg.Reference("id"), int32(3)),
		iceberg.LessThan(iceberg.Reference("id"), int32(13)),
	)
	bound, err := iceberg.BindExpr(schema, expr, true)
	if err != nil {
		b.Fatal(err)
	}
	evaluator, err := newPartitionResidualEvaluator(schema, &spec, bound, true)
	if err != nil {
		b.Fatal(err)
	}
	if evaluator == nil {
		b.Fatal("expected an identity partition residual evaluator")
	}

	const count = 16_384
	for _, pattern := range []string{"runs", "interleaved", "high-cardinality"} {
		b.Run(pattern, func(b *testing.B) {
			partitions := make([]map[int]any, count)
			for i := range partitions {
				var value int32
				switch pattern {
				case "runs":
					value = int32((i / 128) % 16)
				case "interleaved":
					value = int32((i * 7) % 16)
				default:
					value = int32(i)
				}
				partitions[i] = map[int]any{1000: value}
			}

			b.ReportMetric(float64(count), "partitions/op")
			for _, mode := range []string{"baseline", "cached"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						var cache partitionResidualCache[iceberg.BooleanExpression]
						for _, partition := range partitions {
							if mode == "cached" {
								key, supported := partitionResidualKey(spec.ID(), partition)
								if supported {
									if value, found := cache.lookup(key); found {
										if value.changed {
											scanResidualCacheBenchSink = value.value
										} else {
											scanResidualCacheBenchSink = nil
										}
										continue
									}
								}
								residual, changed, err := evaluator.residual(partition)
								if err != nil {
									b.Fatal(err)
								}
								if supported {
									cache.remember(key, residual, changed)
								}
								if changed {
									scanResidualCacheBenchSink = residual
								} else {
									scanResidualCacheBenchSink = nil
								}
								continue
							}

							residual, changed, err := evaluator.residual(partition)
							if err != nil {
								b.Fatal(err)
							}
							if changed {
								scanResidualCacheBenchSink = residual
							} else {
								scanResidualCacheBenchSink = nil
							}
						}
					}
				})
			}
		})
	}
}

// For end-to-end planning costs, also run BenchmarkPlanFilesTaskFinalization
// (plain, identity_residual, split_offsets) on the base and PR branches.

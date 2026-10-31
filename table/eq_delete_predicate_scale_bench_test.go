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

var eqDeleteConflictPredicateScaleSink iceberg.BooleanExpression

// BenchmarkEqDeleteConflictPredicateScale measures the end-to-end predicate
// construction and schema binding used by serializable equality-delete
// conflict checks. Run the same benchmark on the upstream base and this
// branch. No remote catalog, manifest IO, or file reads are included.
func BenchmarkEqDeleteConflictPredicateScale(b *testing.B) {
	for _, tc := range []struct {
		fields     int
		partitions int
	}{
		{fields: 1, partitions: 1},
		{fields: 1, partitions: 16},
		{fields: 1, partitions: 1000},
		{fields: 1, partitions: 4096},
		{fields: 4, partitions: 1000},
		{fields: 4, partitions: 4096},
	} {
		b.Run(fmt.Sprintf("fields=%d/partitions=%d", tc.fields, tc.partitions), func(b *testing.B) {
			meta, files := equalityDeleteConflictBenchmarkInput(
				b, tc.fields, tc.partitions, tc.partitions)
			schema := meta.CurrentSchema()

			b.Run("build", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					filter, err := eqDeletePartitionsToFilter(files, meta)
					if err != nil {
						b.Fatal(err)
					}
					eqDeleteConflictPredicateScaleSink = filter
				}
			})

			b.Run("build_bind", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					filter, err := eqDeletePartitionsToFilter(files, meta)
					if err != nil {
						b.Fatal(err)
					}
					bound, err := iceberg.BindExpr(schema, filter, true)
					if err != nil {
						b.Fatal(err)
					}
					eqDeleteConflictPredicateScaleSink = bound
				}
			})
		})
	}
}

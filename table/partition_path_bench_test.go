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
	"testing"

	"github.com/apache/iceberg-go"
)

var partitionPathFormatBenchmarkResult string

func BenchmarkPartitionPathFormat(b *testing.B) {
	for _, tc := range []struct {
		name       string
		value      string
		fieldCount int
	}{
		{"plain_1", "berlin", 1},
		{"escaped_1", "berlin/east +%?#", 1},
		{"unicode_1", "München/東京", 1},
		{"plain_4", "berlin", 4},
		{"escaped_4", "berlin/east +%?#", 4},
		{"unicode_16", "München/東京", 16},
	} {
		b.Run(tc.name, func(b *testing.B) {
			plan := partitionPathPlan{fields: make([]partitionPathField, tc.fieldCount)}
			values := make(partitionRecord, tc.fieldCount)
			for i := range plan.fields {
				plan.fields[i] = partitionPathField{
					escapedName: "field_name",
					transform:   iceberg.IdentityTransform{},
					resultType:  iceberg.PrimitiveTypes.String,
				}
				values[i] = tc.value
				plan.estimatedSize += len("field_name") + 20
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				partitionPathFormatBenchmarkResult = plan.format(values)
			}
		})
	}
}

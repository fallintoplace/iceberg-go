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

package iceberg_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

var nameMappingStringBenchmarkSink string

func nameMappingStringBenchmarkFlat(count int) iceberg.NameMapping {
	mapping := make(iceberg.NameMapping, count)
	for i := range mapping {
		id := i + 1
		mapping[i] = iceberg.MappedField{
			FieldID: &id,
			Names:   []string{fmt.Sprintf("column_%d", i), fmt.Sprintf("alias_%d", i)},
		}
	}

	return mapping
}

func nameMappingStringBenchmarkNested(depth, fields int) iceberg.NameMapping {
	mapping := nameMappingStringBenchmarkFlat(fields)
	for level := 0; level < depth; level++ {
		id := level + 1
		mapping = iceberg.NameMapping{{
			FieldID: &id,
			Names:   []string{fmt.Sprintf("parent_%d", level), "alias"},
			Fields:  mapping,
		}}
	}

	return mapping
}

func BenchmarkNameMappingString(b *testing.B) {
	cases := []struct {
		name    string
		mapping iceberg.NameMapping
	}{
		{"flat/1", nameMappingStringBenchmarkFlat(1)},
		{"flat/16", nameMappingStringBenchmarkFlat(16)},
		{"flat/256", nameMappingStringBenchmarkFlat(256)},
		{"flat/1024", nameMappingStringBenchmarkFlat(1024)},
		{"nested/4x16", nameMappingStringBenchmarkNested(4, 16)},
		{"nested/8x256", nameMappingStringBenchmarkNested(8, 256)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				nameMappingStringBenchmarkSink = tc.mapping.String()
			}
		})
	}
}

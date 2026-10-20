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

package iceberg

import (
	"strings"
	"testing"
)

var benchmarkVariantPathFields []string

func BenchmarkParseVariantPath(b *testing.B) {
	for _, tt := range []struct {
		name string
		path string
	}{
		{"one", "$['customer_address_123456789']"},
		{"four", "$['country']['region']['city']['zipcode']"},
		{"sixteen", "$" + strings.Repeat("['name_xxxxx']", 16)},
		{"eight_escaped", "$" + strings.Repeat("['a\\'b_012345']", 8)},
	} {
		b.Run(tt.name, func(b *testing.B) {
			b.ReportAllocs()
			var fields []string
			for b.Loop() {
				var err error
				fields, err = parseVariantPath(tt.path)
				if err != nil {
					b.Fatal(err)
				}
			}
			benchmarkVariantPathFields = fields
		})
	}
}

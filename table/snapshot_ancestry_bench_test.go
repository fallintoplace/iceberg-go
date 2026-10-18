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

var benchmarkIsAncestorOfResult bool

func BenchmarkIsAncestorOf(b *testing.B) {
	for _, depth := range []int64{8, 128, 1024} {
		lookup := buildChain(depth)
		for _, tc := range []struct {
			name string
			id   int64
		}{
			{"found", 1},
			{"missing", depth + 1},
		} {
			b.Run(fmt.Sprintf("%d_%s", depth, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				var found bool
				for i := 0; i < b.N; i++ {
					found = IsAncestorOf(depth, tc.id, lookup)
				}
				benchmarkIsAncestorOfResult = found
			})
		}
	}
}

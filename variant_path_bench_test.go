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

import "testing"

var variantSelectorBenchmarkSink string
var variantPathBenchmarkSink []string

func BenchmarkParseBracketSelector(b *testing.B) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"plain_short", "$['event_type']"},
		{"plain_long", "$['this_is_a_long_field_name_which_has_no_escapes']"},
		{"escaped", "$['o\\'brien']"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			selector := tc.path[1:]
			b.ReportAllocs()
			for b.Loop() {
				name, consumed, err := parseBracketSelector(selector, tc.path)
				if err != nil || consumed != len(selector) {
					b.Fatalf("parse bracket selector %q: consumed=%d err=%v", tc.path, consumed, err)
				}
				variantSelectorBenchmarkSink = name
			}
		})
	}
}

func BenchmarkParseVariantPath(b *testing.B) {
	for _, tc := range []struct {
		name string
		path string
	}{
		{"plain_nested", "$['event']['location']['latitude']"},
		{"escaped", "$['event']['o\\'brien']"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				names, err := parseVariantPath(tc.path)
				if err != nil {
					b.Fatal(err)
				}
				variantPathBenchmarkSink = names
			}
		})
	}
}

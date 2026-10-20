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
	"slices"
	"strings"
	"testing"
)

func TestParseBracketSelectorFastPath(t *testing.T) {
	for _, tt := range []struct {
		selector string
		want     string
	}{
		{"['']", ""},
		{"['field']", "field"},
		{"['user.name']", "user.name"},
		{"['naïve-日本語']", "naïve-日本語"},
		{"['a\\b']", "a\b"},
		{"['a\\'b']", "a'b"},
		{"['a\\u0001b']", "a\x01b"},
	} {
		t.Run(tt.selector, func(t *testing.T) {
			// The parser should consume only the first selector.
			path := tt.selector + "['next']"
			got, consumed, err := parseBracketSelector(path, "$"+path)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want || consumed != len(tt.selector) {
				t.Fatalf("parseBracketSelector(%q) = (%q, %d), want (%q, %d)",
					path, got, consumed, tt.want, len(tt.selector))
			}
		})
	}
}

func TestParseVariantPathManySelectors(t *testing.T) {
	fields := make([]string, 0, 20)
	for range 16 {
		fields = append(fields, "nested_field")
	}
	fields = append(fields, "naïve-日本語", "field.with.dot", "quote's", "line\nbreak")
	path := NormalizeVariantPath(fields)
	got, err := parseVariantPath(path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, fields) {
		t.Fatalf("parseVariantPath(%q) = %q, want %q", path, got, fields)
	}

	// Reject a quoted selector without the closing bracket.
	for _, bad := range []string{"$['a'broken]", "$['a'", "$['a\\'b'junk]"} {
		if _, err := parseVariantPath(bad); err == nil {
			t.Fatalf("parseVariantPath(%q) unexpectedly succeeded", bad)
		}
	}

	// The fast path should handle many repeated unescaped member selectors.
	path = "$" + strings.Repeat("['nested_field']", 16)
	got, err = parseVariantPath(path)
	if err != nil || len(got) != 16 {
		t.Fatalf("parseVariantPath(%q) = %q, %v; want 16 fields", path, got, err)
	}
}

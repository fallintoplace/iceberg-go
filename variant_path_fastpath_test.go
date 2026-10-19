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

func TestParseBracketSelectorFastPath(t *testing.T) {
	longName := strings.Repeat("field_name_", 12)
	for _, tt := range []struct {
		name string
		path string
		want string
	}{
		{"empty", "$['']", ""},
		{"short", "$['event_type']", "event_type"},
		{"dotted", "$['user.name']", "user.name"},
		{"double quote", "$['a\"b']", "a\"b"},
		{"unicode", "$['naïve☃']", "naïve☃"},
		{"long", "$['" + longName + "']", longName},
		{"escaped quote", "$['o\\'brien']", "o'brien"},
		{"escaped backslash", "$['a\\\\b']", "a\\b"},
		{"unicode escape", "$['\\u00e9']", "é"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, consumed, err := parseBracketSelector(tt.path[1:], tt.path)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want || consumed != len(tt.path)-1 {
				t.Fatalf("got (%q, %d), want (%q, %d)", got, consumed, tt.want, len(tt.path)-1)
			}
		})
	}
}

func TestParseBracketSelectorFastPathInvalid(t *testing.T) {
	for _, path := range []string{
		"$['missing bracket'",
		"$['early'quote']",
		"$['dangling\\",
		"$['bad\\q']",
		"$['\\uD840']",
	} {
		if _, _, err := parseBracketSelector(path[1:], path); err == nil {
			t.Errorf("expected error for %q", path)
		}
	}
}

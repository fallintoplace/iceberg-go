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

import "testing"

func TestJoinLocationPath(t *testing.T) {
	tests := []struct {
		name     string
		location string
		elem     []string
		want     string
	}{
		{name: "single", location: "dir", elem: []string{"a"}, want: "dir/a"},
		{name: "object store", location: "s3://bucket/table/data", elem: []string{"0101/0110", "part.parquet"}, want: "s3://bucket/table/data/0101/0110/part.parquet"},
		{name: "three components", location: "dir", elem: []string{"a", "b", "c"}, want: "dir/a/b/c"},
		{name: "trailing slash", location: "s3://bucket/table///", elem: []string{"a", "b"}, want: "s3://bucket/table/a/b"},
		{name: "empty location", location: "", elem: []string{"a", "b"}, want: "/a/b"},
		{name: "root", location: "/", elem: []string{"a", "b"}, want: "/a/b"},
		{name: "empty element", location: "dir", elem: []string{"", "b"}, want: "dir//b"},
		{name: "trailing empty element", location: "dir", elem: []string{"a", ""}, want: "dir/a/"},
		{name: "no elements", location: "dir", want: "dir/"},
		{name: "no elements root", location: "///", want: "/"},
		{name: "no elements empty", location: "", want: "/"},
		{name: "literal path", location: "s3://bucket/a/./b/../table///", elem: []string{"a?b", "c#d", "%literal"}, want: "s3://bucket/a/./b/../table/a?b/c#d/%literal"},
		{name: "backslash", location: `C:\table\`, elem: []string{"a", "b"}, want: `C:\table\/a/b`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinLocationPath(tt.location, tt.elem...); got != tt.want {
				t.Errorf("joinLocationPath(%q, %q) = %q, want %q", tt.location, tt.elem, got, tt.want)
			}
		})
	}
}

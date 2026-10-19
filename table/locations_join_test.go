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
	for _, tt := range []struct {
		name  string
		base  string
		elems []string
		want  string
	}{
		{"no components", "s3://bucket/path///", nil, "s3://bucket/path/"},
		{"single component", "s3://bucket/path///", []string{"file.parquet"}, "s3://bucket/path/file.parquet"},
		{"two components", "s3://bucket/path///", []string{"partition=x", "file.parquet"}, "s3://bucket/path/partition=x/file.parquet"},
		{"three components", "s3://bucket/path", []string{"a", "b", "c"}, "s3://bucket/path/a/b/c"},
		{"empty components", "s3://bucket/path", []string{"", ""}, "s3://bucket/path//"},
		{"root location", "/", []string{"a", "b"}, "/a/b"},
		{"empty location", "", []string{"a", "b"}, "/a/b"},
		{"literal paths", "s3://bucket/a//./b/../table///", []string{"dir#?", "file%20.parquet"}, "s3://bucket/a//./b/../table/dir#?/file%20.parquet"},
		{"slashes inside components", "s3://bucket/path", []string{"a/b", "c/d"}, "s3://bucket/path/a/b/c/d"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinLocationPath(tt.base, tt.elems...); got != tt.want {
				t.Errorf("joinLocationPath(%q, %q) = %q, want %q", tt.base, tt.elems, got, tt.want)
			}
		})
	}
}

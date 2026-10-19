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

func TestJoinLocationPathPreservesLiteralComponents(t *testing.T) {
	tests := []struct {
		name     string
		location string
		parts    []string
		want     string
	}{
		{name: "empty", want: "/"},
		{name: "root", location: "///", parts: []string{""}, want: "/"},
		{name: "no_components", location: "s3://bucket/table///", want: "s3://bucket/table/"},
		{name: "one", location: "s3://bucket/table///", parts: []string{"part#?.parquet"}, want: "s3://bucket/table/part#?.parquet"},
		{name: "two", location: "s3://bucket/table///", parts: []string{"000/111", "part#%.parquet"}, want: "s3://bucket/table/000/111/part#%.parquet"},
		{name: "empty_components", location: "/", parts: []string{"", ""}, want: "//"},
		{name: "multiple", location: "dir//./other/../table/", parts: []string{"a", "", "b", "c"}, want: "dir//./other/../table/a//b/c"},
		{name: "windows", location: `C:\table`, parts: []string{"folder", "file"}, want: `C:\table/folder/file`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinLocationPath(tt.location, tt.parts...); got != tt.want {
				t.Errorf("joinLocationPath(%q, %q) = %q, want %q", tt.location, tt.parts, got, tt.want)
			}
		})
	}
}

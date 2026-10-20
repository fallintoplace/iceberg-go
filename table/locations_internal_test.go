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
	for _, tt := range []struct {
		name     string
		location string
		parts    []string
		want     string
	}{
		{
			name: "no-components", location: "s3://bucket/data///",
			want: "s3://bucket/data/",
		},
		{
			name: "root", location: "/", want: "/",
		},
		{
			name: "single", location: "s3://bucket/data///",
			parts: []string{"part-00001.parquet"},
			want: "s3://bucket/data/part-00001.parquet",
		},
		{
			name: "object-store", location: "s3://bucket/data///",
			parts: []string{"0101/1110/0001/10010010", "part-00001.parquet"},
			want: "s3://bucket/data/0101/1110/0001/10010010/part-00001.parquet",
		},
		{
			name: "multiple", location: "gs://bucket/table",
			parts: []string{"metadata", "snapshots", "00001.avro"},
			want: "gs://bucket/table/metadata/snapshots/00001.avro",
		},
		{
			name: "empty-components", location: "file:///tmp///",
			parts: []string{"a", "", "c"},
			want: "file:///tmp/a//c",
		},
		{
			name: "literal-special-characters", location: "s3://bucket/100% #?///",
			parts: []string{"a/../b", "file#?%.parquet"},
			want: "s3://bucket/100% #?/a/../b/file#?%.parquet",
		},
		{
			name: "literal-double-slashes", location: "s3://bucket//./table/",
			parts: []string{"a//b", ".", "c", "d"},
			want: "s3://bucket//./table/a//b/./c/d",
		},
		{
			name: "leading-empty-base", location: "", parts: []string{"a", "b"},
			want: "/a/b",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinLocationPath(tt.location, tt.parts...); got != tt.want {
				t.Fatalf("joinLocationPath(%q, %q) = %q, want %q",
					tt.location, tt.parts, got, tt.want)
			}
		})
	}
}

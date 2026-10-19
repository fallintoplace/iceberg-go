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
	for _, tc := range []struct {
		name, location, want string
		parts                []string
	}{
		{"empty", "", "/", nil},
		{"root-no-components", "///", "/", nil},
		{"single", "s3://bucket/table/data/", "s3://bucket/table/data/file.parquet", []string{"file.parquet"}},
		{"object-store", "s3://bucket/table/data///", "s3://bucket/table/data/hash/dir/file.parquet", []string{"hash/dir", "file.parquet"}},
		{"three-components", "gs://bucket/path//./", "gs://bucket/path//./one/two/three", []string{"one", "two", "three"}},
		{"empty-parts", "data///", "data//", []string{"", ""}},
		{"keep-extra-slashes", "/tmp//", "/tmp/a//b/c/d", []string{"a//b", "c/d"}},
		{"literal-special-characters", "s3://bucket/table?#%/", "s3://bucket/table?#%/1/2", []string{"1", "2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinLocationPath(tc.location, tc.parts...); got != tc.want {
				t.Errorf("joinLocationPath(%q, %q) = %q, want %q", tc.location, tc.parts, got, tc.want)
			}
		})
	}
}

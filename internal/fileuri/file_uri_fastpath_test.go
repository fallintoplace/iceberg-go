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

package fileuri

import (
	"math/rand"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestParsePreservesNetURL(t *testing.T) {
	inputs := []string{
		"file:///tmp/data.parquet",
		"file:///tmp/folder/sub.dir/data_file-01.parquet",
		"file:///tmp/some%20folder/file.parquet",
		"file:///tmp/raw space/file.parquet",
		"file:///tmp/bad%escape",
		"file:///tmp/a?version=1",
		"file:///tmp/a#fragment",
		"file:///C:/Warehouse/data.parquet",
		"file://C:/Warehouse/data.parquet",
		"file:C:/Warehouse%20Space/data.parquet",
		"file:////network/share",
		"file:///tmp/a\\b",
		"file:///tmp/ü/数据.parquet",
		"file:///tmp/a\tname",
		"file:///tmp/a:b+c@d",
		"file:///",
		"FILE:///tmp/data.parquet",
	}
	for ch := 0; ch < 256; ch++ {
		inputs = append(inputs, "file:///tmp/"+string([]byte{byte(ch)})+"/data")
	}

	rng := rand.New(rand.NewSource(20261020))
	const alphabet = "abcXYZ09/:_.+-@ \\?#[%\t\n\x7f"
	for i := 0; i < 2000; i++ {
		var s strings.Builder
		s.WriteString("file:///")
		for n := rng.Intn(80); n > 0; n-- {
			s.WriteByte(alphabet[rng.Intn(len(alphabet))])
		}
		inputs = append(inputs, s.String())
	}

	for _, raw := range inputs {
		expected, expectedErr := url.Parse(raw)
		got, gotErr := Parse(raw)
		if expectedErr != nil {
			if gotErr == nil || expectedErr.Error() != gotErr.Error() {
				t.Fatalf("Parse(%q) error=%v; want %v", raw, gotErr, expectedErr)
			}
			continue
		}
		if gotErr != nil {
			t.Fatalf("Parse(%q) error=%v", raw, gotErr)
		}
		if !reflect.DeepEqual(got.parsed, *expected) {
			t.Fatalf("Parse(%q) URL=%#v; want %#v", raw, got.parsed, *expected)
		}
		wantPath := expected.Path
		if expected.Opaque != "" {
			var err error
			wantPath, err = url.PathUnescape(expected.Opaque)
			if err != nil {
				t.Fatalf("PathUnescape(%q): %v", raw, err)
			}
		}
		if got.path != wantPath {
			t.Fatalf("Parse(%q) path=%q; want %q", raw, got.path, wantPath)
		}
		if expected.Opaque == "" {
			wantJoin := expected.JoinPath("metadata", "hint.text").String()
			if gotJoin := got.JoinPath("metadata", "hint.text"); gotJoin != wantJoin {
				t.Fatalf("JoinPath(%q)=%q; want %q", raw, gotJoin, wantJoin)
			}
		}
	}
}

var parseFileURIBenchmarkResult FileURI

func BenchmarkParseFileURI(b *testing.B) {
	cases := []struct {
		name string
		uri  string
	}{
		{"local_short", "file:///tmp/data.parquet"},
		{"local_long", "file:///tmp/warehouse/db/table/data/snap-00001234-682b4531-cb82-4884-b15d-9f33bd6b12fb.parquet"},
		{"unescaped_space", "file:///tmp/some folder/a.parquet"},
		{"escaped_path", "file:///tmp/some%20folder/a.parquet"},
		{"windows_authority", "file://C:/Warehouse/data.parquet"},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				var err error
				parseFileURIBenchmarkResult, err = Parse(tc.uri)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

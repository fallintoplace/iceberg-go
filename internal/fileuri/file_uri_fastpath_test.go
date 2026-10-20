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

func TestParseSimpleFileURIParity(t *testing.T) {
	cases := []string{
		"file:///",
		"file:///tmp/table/metadata/00001-abc.metadata.json",
		"file:///C:/warehouse/table/data.parquet",
		"file:////shared/table/data.parquet",
		"file:///a_b-c.d~e:123/partition=4",
		"file:///a%20b/data.parquet",
		"file:///a%2Fb",
		"file:///a?query=1",
		"file:///a#fragment",
		"file:///name+value",
		"file:///name@value",
		"file:///name!value",
		"file:///é/data.parquet",
		"file://localhost/table/data.parquet",
		"file://C:/warehouse/data.parquet",
		"file:C:/warehouse/data.parquet",
		"FILE:///tmp/table",
		"file:///invalid%ZZ",
		"file:///invalid\x00path",
	}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			assertFileURIParity(t, name)
		})
	}

	// Compare against net/url for a wide range of fast-path characters.
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789/-_.~:"
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 2000; i++ {
		n := 1 + r.Intn(128)
		var builder strings.Builder
		builder.Grow(len("file:///") + n)
		builder.WriteString("file:///")
		for j := 0; j < n; j++ {
			builder.WriteByte(chars[r.Intn(len(chars))])
		}
		assertFileURIParity(t, builder.String())
	}
}

func assertFileURIParity(t *testing.T, name string) {
	t.Helper()
	wantURL, wantErr := url.Parse(name)
	got, gotErr := Parse(name)
	if (gotErr != nil) != (wantErr != nil) {
		t.Fatalf("Parse(%q) error = %v; net/url error = %v", name, gotErr, wantErr)
	}
	if wantErr != nil {
		return
	}

	if !reflect.DeepEqual(got.parsed, *wantURL) {
		t.Fatalf("Parse(%q) URL = %#v; want %#v", name, got.parsed, *wantURL)
	}
	wantPath := wantURL.Path
	if wantURL.Opaque != "" {
		var err error
		wantPath, err = url.PathUnescape(wantURL.Opaque)
		if err != nil {
			t.Fatal(err)
		}
	}
	if got.LocalPath(false) != wantPath {
		t.Fatalf("Parse(%q) path = %q; want %q", name, got.LocalPath(false), wantPath)
	}

	wantJoin := wantURL.JoinPath("metadata", "version-hint.text").String()
	if joined := got.JoinPath("metadata", "version-hint.text"); joined != wantJoin {
		t.Fatalf("Parse(%q) join = %q; want %q", name, joined, wantJoin)
	}
}

var benchmarkParsedFileURI FileURI

func BenchmarkParseFileURI(b *testing.B) {
	cases := []struct {
		name, input string
	}{
		{"plain", "file:///warehouse/namespace/table/metadata/00012-12345678-abcd.parquet"},
		{"deep", "file:///warehouse/" + strings.Repeat("table/partition_col_1234/", 15) + "00012.parquet"},
		{"windows", "file:///C:/warehouse/data.parquet"},
		{"escaped", "file:///warehouse/spaced%20name/data.parquet"},
		{"authority", "file://localhost/warehouse/data.parquet"},
		{"opaque", "file:C:/data/file.parquet"},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				parsed, err := Parse(tc.input)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkParsedFileURI = parsed
			}
		})
	}
}

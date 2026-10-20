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

package io_test

import (
	"io/fs"
	"slices"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

func TestMemIO_WalkDirSharedAncestorsAndFileShadow(t *testing.T) {
	memIO := icebergio.NewMemFS()
	for _, name := range []string{
		"mem://bucket/root/a/b/c/1.parquet",
		"mem://bucket/root/a/b/c/2.parquet",
		"mem://bucket/root/a/b/d/3.parquet",
		"mem://bucket/root/a/b",
	} {
		if err := memIO.WriteFile(name, nil); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	err := memIO.WalkDir("mem://bucket/root", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		kind := "D:"
		if !entry.IsDir() {
			kind = "F:"
		}
		got = append(got, kind+name)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	want := []string{
		"D:mem://bucket/root",
		"D:mem://bucket/root/a",
		"F:mem://bucket/root/a/b",
		"D:mem://bucket/root/a/b/c",
		"F:mem://bucket/root/a/b/c/1.parquet",
		"F:mem://bucket/root/a/b/c/2.parquet",
		"D:mem://bucket/root/a/b/d",
		"F:mem://bucket/root/a/b/d/3.parquet",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("WalkDir entries mismatch:\n got: %v\nwant: %v", got, want)
	}
}

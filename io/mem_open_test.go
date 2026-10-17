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
	"bytes"
	"errors"
	"io"
	"io/fs"
	"sync"
	"testing"

	icebergio "github.com/apache/iceberg-go/io"
)

func TestMemFSOpenRetainsSnapshotAfterChanges(t *testing.T) {
	memIO := icebergio.NewMemFS()
	const name = "mem://bucket/snapshot.parquet"
	original := []byte("before")
	if err := memIO.WriteFile(name, original); err != nil {
		t.Fatal(err)
	}
	original[0] = 'X'

	first, err := memIO.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	replacement := []byte("after!")
	if err := memIO.WriteFile(name, replacement); err != nil {
		t.Fatal(err)
	}
	replacement[0] = 'Y'

	second, err := memIO.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	if err := memIO.Remove(name); err != nil {
		t.Fatal(err)
	}
	if _, err := memIO.Open(name); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("opening removed file: got %v, want not-exist", err)
	}

	for _, tc := range []struct {
		file io.Reader
		want string
	}{
		{first, "before"},
		{second, "after!"},
	} {
		got, err := io.ReadAll(tc.file)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != tc.want {
			t.Fatalf("snapshot changed: got %q, want %q", got, tc.want)
		}
	}
}

func TestMemFSOpenConcurrentWithOverwrite(t *testing.T) {
	memIO := icebergio.NewMemFS()
	const name = "mem://bucket/overwrite.parquet"
	first := bytes.Repeat([]byte{'a'}, 16<<10)
	second := bytes.Repeat([]byte{'b'}, 16<<10)
	if err := memIO.WriteFile(name, first); err != nil {
		t.Fatal(err)
	}

	const iterations = 256
	var wg sync.WaitGroup
	failures := make(chan error, 5)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			value := first
			if i%2 != 0 {
				value = second
			}
			if err := memIO.WriteFile(name, value); err != nil {
				failures <- err
				return
			}
		}
	}()

	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				file, err := memIO.Open(name)
				if err != nil {
					failures <- err
					return
				}
				got, readErr := io.ReadAll(file)
				closeErr := file.Close()
				if readErr != nil {
					failures <- readErr
					return
				}
				if closeErr != nil {
					failures <- closeErr
					return
				}
				if !bytes.Equal(got, first) && !bytes.Equal(got, second) {
					failures <- errors.New("read mixed file versions")
					return
				}
			}
		}()
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

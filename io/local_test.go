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

package io

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalFSWalkDir(t *testing.T) {
	dir := t.TempDir()

	// Create a directory structure.
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "data"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "metadata"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data", "file1.parquet"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "data", "file2.parquet"), []byte("b"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "metadata", "v1.json"), []byte("c"), 0o644))

	lfs := LocalFS{}

	var files []string
	err := lfs.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)

	sort.Strings(files)
	expected := []string{
		filepath.Join(dir, "data", "file1.parquet"),
		filepath.Join(dir, "data", "file2.parquet"),
		filepath.Join(dir, "metadata", "v1.json"),
	}
	sort.Strings(expected)
	assert.Equal(t, expected, files)
}

func TestLocalFSWalkDirWithFileScheme(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "test.txt"), []byte("hello"), 0o644))

	lfs := LocalFS{}

	var files []string
	err := lfs.WalkDir("file://"+dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			files = append(files, path)
		}

		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{filepath.Join(dir, "test.txt")}, files)
}

func TestLocalFSWriteFileCreatesParentDirectories(t *testing.T) {
	dir := t.TempDir()
	content := []byte("content")

	for _, tt := range []struct {
		name string
		path string
	}{
		{
			name: "plain path",
			path: filepath.Join(dir, "plain", "nested", "file.txt"),
		},
		{
			name: "file scheme",
			path: "file://" + filepath.Join(dir, "scheme", "nested", "file.txt"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, LocalFS{}.WriteFile(tt.path, content))

			got, err := os.ReadFile(strings.TrimPrefix(tt.path, "file://"))
			require.NoError(t, err)
			assert.Equal(t, content, got)
		})
	}
}

func TestLocalFSImplementsListableIO(t *testing.T) {
	var _ ListableIO = LocalFS{}
}

func TestLocalFilePath(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"plain path", "/tmp/file.parquet", "/tmp/file.parquet"},
		{"file scheme", "file:///tmp/file.parquet", "/tmp/file.parquet"},
		{"relative file scheme", "file://relative/file.parquet", "relative/file.parquet"},
		{"case sensitive", "FILE:///tmp/file.parquet", "FILE:///tmp/file.parquet"},
		{"only one prefix", "file://file://file.parquet", "file://file.parquet"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, localFilePath(tt.input))
		})
	}
}

func TestLocalFSCreateCreatesParentDirectories(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix string
	}{
		{"plain path", ""},
		{"file scheme", "file://"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "nested", "file.parquet")
			writer, err := (LocalFS{}).Create(tt.prefix + filename)
			require.NoError(t, err)
			_, err = writer.Write([]byte("data"))
			require.NoError(t, err)
			require.NoError(t, writer.Close())

			got, err := os.ReadFile(filename)
			require.NoError(t, err)
			assert.Equal(t, []byte("data"), got)
		})
	}
}

func TestLocalFSFileSchemeOperations(t *testing.T) {
	for _, tt := range []struct {
		name   string
		prefix string
	}{
		{"plain path", ""},
		{"file scheme", "file://"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lfs := LocalFS{}
			root := filepath.Join(t.TempDir(), "table")
			asPath := func(name string) string { return tt.prefix + name }

			nested := filepath.Join(root, "nested")
			require.NoError(t, lfs.MkdirAll(asPath(nested)))
			require.NoError(t, lfs.Mkdir(asPath(filepath.Join(root, "empty"))))

			before := filepath.Join(nested, "before.parquet")
			after := filepath.Join(nested, "after.parquet")
			require.NoError(t, lfs.WriteFile(asPath(before), []byte("data")))

			got, err := lfs.ReadFile(asPath(before))
			require.NoError(t, err)
			assert.Equal(t, []byte("data"), got)

			file, err := lfs.Open(asPath(before))
			require.NoError(t, err)
			require.NoError(t, file.Close())

			info, err := lfs.Stat(asPath(before))
			require.NoError(t, err)
			assert.Equal(t, int64(4), info.Size())

			entries, err := lfs.ReadDir(asPath(root))
			require.NoError(t, err)
			assert.Len(t, entries, 2)

			require.NoError(t, lfs.Rename(asPath(before), asPath(after)))
			require.NoError(t, lfs.Remove(asPath(after)))
			require.NoError(t, lfs.RemoveAll(asPath(root)))
			_, err = os.Stat(root)
			require.ErrorIs(t, err, fs.ErrNotExist)
		})
	}
}

func TestLocalFSWriteParentDirectoryError(t *testing.T) {
	parent := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(parent, []byte("existing"), 0o644))
	path := filepath.Join(parent, "nested.parquet")

	for _, prefix := range []string{"", "file://"} {
		_, err := (LocalFS{}).Create(prefix + path)
		require.Error(t, err)
		require.Error(t, (LocalFS{}).WriteFile(prefix+path, []byte("data")))
	}
}

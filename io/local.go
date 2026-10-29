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
	"strings"
)

// LocalFS is an implementation of IO that implements interaction with
// the local file system.
type LocalFS struct{}

// localFilePath preserves LocalFS's historical literal file:// prefix handling.
func localFilePath(name string) string {
	return strings.TrimPrefix(name, "file://")
}

// prepareLocalWritePath resolves a local path and creates its parent directories.
func prepareLocalWritePath(name string) (string, error) {
	filename := localFilePath(name)
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return "", err
	}

	return filename, nil
}

func (LocalFS) Open(name string) (File, error) {
	return os.Open(localFilePath(name))
}

func (LocalFS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(localFilePath(name))
}

func (LocalFS) Create(name string) (FileWriter, error) {
	filename, err := prepareLocalWritePath(name)
	if err != nil {
		return nil, err
	}

	return os.Create(filename)
}

func (LocalFS) WriteFile(name string, content []byte) error {
	filename, err := prepareLocalWritePath(name)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, content, 0o644)
}

func (LocalFS) Remove(name string) error {
	return os.Remove(localFilePath(name))
}

func (LocalFS) RemoveAll(name string) error {
	return os.RemoveAll(localFilePath(name))
}

func (LocalFS) WalkDir(root string, fn fs.WalkDirFunc) error {
	return filepath.WalkDir(localFilePath(root), fn)
}

func (LocalFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return os.ReadDir(localFilePath(name))
}

func (LocalFS) MkdirAll(path string) error {
	return os.MkdirAll(localFilePath(path), 0o755)
}

func (LocalFS) Mkdir(path string) error {
	return os.Mkdir(localFilePath(path), 0o755)
}

func (LocalFS) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(localFilePath(name))
}

func (LocalFS) Rename(oldpath, newpath string) error {
	return os.Rename(localFilePath(oldpath), localFilePath(newpath))
}

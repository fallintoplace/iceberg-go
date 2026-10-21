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

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func inspectPrefetchFilePaths(t *testing.T, tbl *Table, concurrency int) []string {
	t.Helper()

	inspect := tbl.Inspect()
	inspect.manifestPrefetchConcurrency = concurrency
	reader, err := inspect.Files(t.Context())
	require.NoError(t, err)
	defer reader.Release()

	pathIndex := -1
	for i, field := range reader.Schema().Fields() {
		if field.Name == "file_path" {
			pathIndex = i
			break
		}
	}
	require.NotEqual(t, -1, pathIndex)

	var paths []string
	for reader.Next() {
		col, ok := reader.RecordBatch().Column(pathIndex).(*array.String)
		require.True(t, ok)
		for row := range int(reader.RecordBatch().NumRows()) {
			paths = append(paths, col.Value(row))
		}
	}
	require.NoError(t, reader.Err())

	return paths
}

func TestInspectManifestPrefetchParallelFileRows(t *testing.T) {
	for _, count := range []int{1, 8, 17} {
		t.Run(fmt.Sprintf("manifests=%d", count), func(t *testing.T) {
			tbl, tracked := inspectWithTrackingIO(t, inspectParallelPartitionManifestTable(t, count))

			serial := inspectPrefetchFilePaths(t, tbl, 1)
			prefetched := inspectPrefetchFilePaths(t, tbl, inspectManifestPrefetchConcurrency)
			require.Equal(t, serial, prefetched)
			require.Len(t, prefetched, count*2)
			require.LessOrEqual(t, tracked.maxActive.Load(), int32(inspectManifestPrefetchConcurrency))
			if count > 1 {
				require.GreaterOrEqual(t, tracked.maxActive.Load(), int32(2))
			}
			require.Zero(t, tracked.active.Load())
		})
	}
}

func TestInspectManifestPrefetchOpenError(t *testing.T) {
	tbl, tracked := inspectWithTrackingIO(t, inspectParallelPartitionManifestTable(t, 8))
	tracked.failPath = "inspect-3.avro"
	reader, err := tbl.Inspect().Files(t.Context())
	require.NoError(t, err)
	defer reader.Release()

	for reader.Next() {
	}
	require.ErrorContains(t, reader.Err(), "injected manifest read error")
	require.Zero(t, tracked.active.Load())
}

func TestInspectManifestPrefetchEarlyStop(t *testing.T) {
	tbl, tracked := inspectWithTrackingIO(t, inspectParallelPartitionManifestTable(t, 16))
	provider := tbl.ManifestProvider()
	snapshot := tbl.Metadata().CurrentSnapshot()
	require.NotNil(t, snapshot)
	manifests, err := provider.Manifests(t.Context(), *snapshot)
	require.NoError(t, err)
	fs, err := provider.FS(t.Context())
	require.NoError(t, err)

	source := func(yield func(iceberg.ManifestFile, error) bool) {
		for manifest := range manifests {
			if !yield(manifest, nil) {
				return
			}
		}
	}
	seen := 0
	for manifest, err := range inspectManifestPrefetchSource(
		t.Context(), fs, source, nil, inspectManifestPrefetchConcurrency) {
		require.NoError(t, err)
		require.NotNil(t, manifest.manifest)
		seen++
		break
	}

	require.Equal(t, 1, seen)
	require.LessOrEqual(t, tracked.maxActive.Load(), int32(inspectManifestPrefetchConcurrency))
	require.Zero(t, tracked.active.Load())
}

type inspectOversizeManifest struct{ iceberg.ManifestFile }

func (m inspectOversizeManifest) Length() int64 {
	return inspectManifestPrefetchMaxBytes + 1
}

func TestInspectManifestPrefetchLargeManifestStreams(t *testing.T) {
	tbl := inspectParallelPartitionManifestTable(t, 2)
	provider := tbl.ManifestProvider()
	snapshot := tbl.Metadata().CurrentSnapshot()
	require.NotNil(t, snapshot)
	manifests, err := provider.Manifests(t.Context(), *snapshot)
	require.NoError(t, err)
	fs, err := provider.FS(t.Context())
	require.NoError(t, err)

	source := func(yield func(iceberg.ManifestFile, error) bool) {
		for manifest := range manifests {
			if !yield(inspectOversizeManifest{manifest}, nil) {
				return
			}
		}
	}
	count := 0
	for prefetched, err := range inspectManifestPrefetchSource(
		context.Background(), fs, source, nil, inspectManifestPrefetchConcurrency) {
		require.NoError(t, err)
		require.Equal(t, fs, prefetched.fileIO)
		count++
	}
	require.Equal(t, 2, count)
}

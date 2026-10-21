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

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// parallelDeadPositionDeleteTable has interleaved data and delete manifests.
// Odd deletes are partition-scoped and must be retained because odd data files
// survive. Even deletes have a single rewritten target and are safe to remove.
func parallelDeadPositionDeleteTable(t *testing.T, count int) (*Table, map[string]struct{}, []string) {
	t.Helper()

	spec := *iceberg.UnpartitionedSpec
	snapshotID, dataSeq, deleteSeq := int64(1), int64(1), int64(2)
	manifests := make([]inspectManifestSpec, 0, 2*count)
	rewritten := make(map[string]struct{})
	expected := make([]string, 0, (count+1)/2)

	for index := range count {
		dataPath := fmt.Sprintf("mem://default/table-location/data/target-%d.parquet", index)
		data, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData,
			dataPath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)

		deletePath := fmt.Sprintf("mem://default/table-location/data/delete-%d.parquet", index)
		del, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentPosDeletes,
			deletePath, iceberg.ParquetFile, nil, nil, nil, 1, 128)
		require.NoError(t, err)
		if index%2 == 0 {
			del.ReferencedDataFile(dataPath)
			rewritten[dataPath] = struct{}{}
			expected = append(expected, deletePath)
		}

		manifests = append(manifests,
			inspectManifestSpec{
				content: iceberg.ManifestContentDeletes,
				entries: []iceberg.ManifestEntry{iceberg.NewManifestEntry(
					iceberg.EntryStatusADDED, &snapshotID, &deleteSeq, &deleteSeq, del.Build(),
				)},
			},
			inspectManifestSpec{
				content: iceberg.ManifestContentData,
				entries: []iceberg.ManifestEntry{iceberg.NewManifestEntry(
					iceberg.EntryStatusADDED, &snapshotID, &dataSeq, &dataSeq, data.Build(),
				)},
			},
		)
	}

	return inspectFilesTableWithManifests(t, spec, manifests...), rewritten, expected
}

func TestParallelDeadPositionDeleteManifestReads(t *testing.T) {
	for _, count := range []int{1, 8, 17} {
		t.Run(fmt.Sprintf("manifests=%d", count), func(t *testing.T) {
			base, rewritten, expected := parallelDeadPositionDeleteTable(t, count)
			tbl, tracking := inspectWithTrackingIO(t, base)
			fs, err := tbl.FS(t.Context())
			require.NoError(t, err)

			dead, err := CollectDeadPositionDeletes(t.Context(), fs, tbl.CurrentSnapshot(), rewritten)
			require.NoError(t, err)

			got := make([]string, 0, len(dead))
			for _, df := range dead {
				got = append(got, df.FilePath())
			}
			require.Equal(t, expected, got,
				"merge must preserve manifest order and retain partition-scoped deletes")
			require.LessOrEqual(t, tracking.maxActive.Load(), int32(deadPositionDeleteManifestConcurrency))
			if count > 1 {
				require.GreaterOrEqual(t, tracking.maxActive.Load(), int32(2))
			}
			require.Zero(t, tracking.active.Load())
		})
	}
}

func TestParallelDeadPositionDeleteManifestReadError(t *testing.T) {
	base, rewritten, _ := parallelDeadPositionDeleteTable(t, 17)
	tbl, tracking := inspectWithTrackingIO(t, base)
	tracking.failPath = "inspect-2.avro"
	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)

	dead, err := CollectDeadPositionDeletes(t.Context(), fs, tbl.CurrentSnapshot(), rewritten)
	require.ErrorContains(t, err, "injected manifest read error")
	require.Nil(t, dead)
	require.Zero(t, tracking.active.Load())
}

func TestParallelDeadPositionDeleteManifestCancellation(t *testing.T) {
	base, rewritten, _ := parallelDeadPositionDeleteTable(t, 17)
	tbl, _ := inspectWithTrackingIO(t, base)
	fs, err := tbl.FS(t.Context())
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	dead, err := CollectDeadPositionDeletes(ctx, fs, tbl.CurrentSnapshot(), rewritten)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, dead)
}

// Even when a snapshot has data manifests, file-scoped deletes do not need a
// survivor pass. An IO wrapper that rejects data manifests pins this property.
type rejectDeadPosDataManifestIO struct {
	iceio.IO
}

func (f rejectDeadPosDataManifestIO) Open(path string) (iceio.File, error) {
	for index := 1; index < 34; index += 2 {
		if path == fmt.Sprintf("mem://default/table-location/metadata/inspect-%d.avro", index) {
			return nil, fmt.Errorf("unexpected data manifest read: %s", path)
		}
	}

	return f.IO.Open(path)
}

func TestParallelDeadPositionDeleteSkipsDataWhenFileScoped(t *testing.T) {
	base, rewritten, _ := parallelDeadPositionDeleteTable(t, 8)
	fs, err := base.FS(t.Context())
	require.NoError(t, err)
	manifestList, err := base.CurrentSnapshot().Manifests(fs)
	require.NoError(t, err)

	// Only include file-scoped delete manifests in the selected set. The
	// synthetic data manifests remain present but need not be opened.
	selected := make([]iceberg.ManifestFile, 0, len(manifestList))
	for index, manifest := range manifestList {
		if index%4 == 0 {
			selected = append(selected, manifest)
		} else if manifest.ManifestContent() == iceberg.ManifestContentData {
			selected = append(selected, manifest)
		}
	}

	candidates, hasPartitionScoped, err := collectPositionDeleteCandidates(t.Context(),
		rejectDeadPosDataManifestIO{IO: fs}, selected)
	require.NoError(t, err)
	require.False(t, hasPartitionScoped)
	require.Len(t, candidates, 4)
	require.NotEmpty(t, rewritten)
}

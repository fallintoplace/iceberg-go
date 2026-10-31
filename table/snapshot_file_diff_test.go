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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestDiffSnapshotFilesAppendAndReverse(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	forward, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Equal(t, int64(1), forward.FromSnapshotID)
	require.Equal(t, int64(2), forward.ToSnapshotID)
	require.Empty(t, forward.Removed)
	require.Len(t, forward.Added, 1)
	require.Equal(t, "mem://default/table-location/data-2.parquet", forward.Added[0].File.FilePath())
	require.Equal(t, iceberg.EntryContentData, forward.Added[0].File.ContentType())
	require.Equal(t, int64(2), forward.Added[0].AddedSnapshotID)
	require.Equal(t, int64(2), forward.Added[0].DataSequenceNumber)
	require.NotNil(t, forward.Added[0].FileSequenceNumber)
	require.Equal(t, int64(2), *forward.Added[0].FileSequenceNumber)

	reverse, err := tbl.DiffSnapshotFiles(context.Background(), 2, 1)
	require.NoError(t, err)
	require.Empty(t, reverse.Added)
	require.Len(t, reverse.Removed, 1)
	require.Equal(t, forward.Added[0].File.FilePath(), reverse.Removed[0].File.FilePath())

	same, err := tbl.DiffSnapshotFiles(context.Background(), 1, 1)
	require.NoError(t, err)
	require.Empty(t, same.Added)
	require.Empty(t, same.Removed)
}

func TestDiffSnapshotFilesMissingSnapshotsAndCancellation(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	_, err := tbl.DiffSnapshotFiles(context.Background(), 999, 2)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "starting snapshot")

	_, err = tbl.DiffSnapshotFiles(context.Background(), 1, 999)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "ending snapshot")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tbl.DiffSnapshotFiles(ctx, 1, 2)
	require.ErrorIs(t, err, context.Canceled)

	empty := Table{}
	_, err = empty.DiffSnapshotFiles(context.Background(), 1, 2)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

// The two snapshots deliberately share *no manifest paths*: one manifest was
// rewritten with EXISTING entries and an ADDED entry. The unchanged file must
// cancel even when it has different manifest status and path on each side.
func TestDiffSnapshotFilesManifestRewriteIsNotFileChange(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	writable := fs.(iceio.WriteFileIO)
	before, err := tbl.SnapshotByID(1).Manifests(fs)
	require.NoError(t, err)
	after, err := tbl.SnapshotByID(2).Manifests(fs)
	require.NoError(t, err)
	require.Len(t, before, 1)
	require.Len(t, after, 2)

	first := mustOneLiveFile(t, fs, before[0])
	second := mustOneLiveFile(t, fs, after[1])
	previousSnapshotID, nextSnapshotID := int64(1), int64(2)
	one := int64(1)
	manifestPath := "mem://default/table-location/metadata/rewritten-manifest.avro"
	var manifestData bytes.Buffer
	rewritten, err := iceberg.WriteManifest(manifestPath, &manifestData, 2,
		partitionedSpec(), simpleSchema(), nextSnapshotID, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusEXISTING, &previousSnapshotID, &one, &one, first),
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &nextSnapshotID, nil, nil, second),
		})
	require.NoError(t, err)
	require.NoError(t, writable.WriteFile(context.Background(), manifestPath, manifestData.Bytes()))
	replaceSnapshotManifestList(t, tbl, 2, []iceberg.ManifestFile{rewritten})

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Empty(t, diff.Removed)
	require.Len(t, diff.Added, 1)
	require.Equal(t, second.FilePath(), diff.Added[0].File.FilePath())
}

func TestDiffSnapshotFilesDeletedManifestEntriesNotLive(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	writable := fs.(iceio.WriteFileIO)
	before, err := tbl.SnapshotByID(1).Manifests(fs)
	require.NoError(t, err)
	after, err := tbl.SnapshotByID(2).Manifests(fs)
	require.NoError(t, err)
	first := mustOneLiveFile(t, fs, before[0])
	second := mustOneLiveFile(t, fs, after[1])
	previousSnapshotID, nextSnapshotID := int64(1), int64(2)
	one := int64(1)
	manifestPath := "mem://default/table-location/metadata/removed-manifest.avro"
	var manifestData bytes.Buffer
	replacement, err := iceberg.WriteManifest(manifestPath, &manifestData, 2,
		partitionedSpec(), simpleSchema(), nextSnapshotID, []iceberg.ManifestEntry{
			iceberg.NewManifestEntry(iceberg.EntryStatusDELETED, &nextSnapshotID, &one, &one, first),
			iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &nextSnapshotID, nil, nil, second),
		})
	require.NoError(t, err)
	require.NoError(t, writable.WriteFile(context.Background(), manifestPath, manifestData.Bytes()))
	replaceSnapshotManifestList(t, tbl, 2, []iceberg.ManifestFile{replacement})

	// The snapshot-1 file is no longer live, even though the new manifest
	// contains its tombstone with a populated data sequence number.
	diff, err := tbl.DiffSnapshotFiles(context.Background(), previousSnapshotID, nextSnapshotID)
	require.NoError(t, err)
	require.Len(t, diff.Added, 1)
	require.Len(t, diff.Removed, 1)
	require.Equal(t, second.FilePath(), diff.Added[0].File.FilePath())
	require.Equal(t, first.FilePath(), diff.Removed[0].File.FilePath())
}

func TestDiffSnapshotFilesSharedSourcesAvoidBackend(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	// Snapshot 2 points to exactly the same immutable manifest list as 1.
	// No file system is needed to prove the inventories are identical.
	builder, err := MetadataBuilderFromBase(tbl.metadata, "")
	require.NoError(t, err)
	for i := range builder.snapshotList {
		if builder.snapshotList[i].SnapshotID == 2 {
			builder.snapshotList[i].ManifestList = tbl.SnapshotByID(1).ManifestList
		}
	}
	tbl.metadata, err = builder.Build()
	require.NoError(t, err)
	tbl.fsF = nil

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Empty(t, diff.Added)
	require.Empty(t, diff.Removed)
}

func TestSameSnapshotManifestSourcesEmbeddedV1(t *testing.T) {
	a := Snapshot{ManifestLocations: []string{"a.avro", "b.avro"}}
	b := Snapshot{ManifestLocations: []string{"a.avro", "b.avro"}}
	require.True(t, sameSnapshotManifestSources(a, b))
	b.ManifestLocations = []string{"b.avro", "a.avro"}
	require.False(t, sameSnapshotManifestSources(a, b))
	require.False(t, sameSnapshotManifestSources(Snapshot{}, Snapshot{ManifestLocations: []string{}}))
	require.False(t, sameSnapshotManifestSources(Snapshot{ManifestList: "list.avro"}, a))
}

func TestDiffSnapshotFilesMissingFileIO(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	tbl.fsF = nil
	// No shortcut applies because snapshot 2 has a different manifest list.
	_, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.True(t, errors.Is(err, ErrInvalidOperation))
}

// A snapshot comparison is an inventory diff, not an ancestry walk. Branch
// heads that share a parent must report both the files gained and lost.
func TestDiffSnapshotFilesDivergentBranches(t *testing.T) {
	tbl := incrementalAppendTestTable(t)
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	writable := fs.(iceio.WriteFileIO)
	one := int64(1)
	three := int64(3)
	newFile := newTestDataFile(t, partitionedSpec(),
		"mem://default/table-location/data-3.parquet", map[int]any{1000: int32(3)})
	manifestPath := "mem://default/table-location/metadata/branch-3.avro"
	var buf bytes.Buffer
	manifest, err := iceberg.WriteManifest(manifestPath, &buf, 2, partitionedSpec(), simpleSchema(), three,
		[]iceberg.ManifestEntry{iceberg.NewManifestEntry(iceberg.EntryStatusADDED, &three, nil, nil, newFile)})
	require.NoError(t, err)
	require.NoError(t, writable.WriteFile(context.Background(), manifestPath, buf.Bytes()))

	oldManifests, err := tbl.SnapshotByID(1).Manifests(fs)
	require.NoError(t, err)
	listPath := "mem://default/table-location/metadata/branch-snap-3.avro"
	buf.Reset()
	require.NoError(t, iceberg.WriteManifestList(2, &buf, three, &one, &three, 0,
		append(oldManifests, manifest)))
	require.NoError(t, writable.WriteFile(context.Background(), listPath, buf.Bytes()))

	builder, err := MetadataBuilderFromBase(tbl.metadata, "")
	require.NoError(t, err)
	schemaID := simpleSchema().ID
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: three, ParentSnapshotID: &one, SequenceNumber: three,
		TimestampMs: tbl.metadata.LastUpdatedMillis() + 1, ManifestList: listPath,
		SchemaID: &schemaID, Summary: &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef("dev", three, BranchRef))
	tbl.metadata, err = builder.Build()
	require.NoError(t, err)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 2, 3)
	require.NoError(t, err)
	require.Len(t, diff.Added, 1)
	require.Len(t, diff.Removed, 1)
	require.Equal(t, newFile.FilePath(), diff.Added[0].File.FilePath())
	require.Equal(t, "mem://default/table-location/data-2.parquet", diff.Removed[0].File.FilePath())
}

func mustOneLiveFile(t *testing.T, fs iceio.IO, manifest iceberg.ManifestFile) iceberg.DataFile {
	t.Helper()
	var files []iceberg.DataFile
	for entry, err := range manifest.Entries(fs, true) {
		require.NoError(t, err)
		files = append(files, entry.DataFile())
	}
	require.Len(t, files, 1)
	return files[0]
}

func replaceSnapshotManifestList(t *testing.T, tbl *Table, snapshotID int64, manifests []iceberg.ManifestFile) {
	t.Helper()
	fs, err := tbl.FS(context.Background())
	require.NoError(t, err)
	writable := fs.(iceio.WriteFileIO)
	path := "mem://default/table-location/metadata/rewrite-snap-2.avro"
	var buf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(2, &buf, snapshotID, nil, &snapshotID, 0, manifests))
	require.NoError(t, writable.WriteFile(context.Background(), path, buf.Bytes()))

	builder, err := MetadataBuilderFromBase(tbl.metadata, "")
	require.NoError(t, err)
	for i := range builder.snapshotList {
		if builder.snapshotList[i].SnapshotID == snapshotID {
			builder.snapshotList[i].ManifestList = path
		}
	}
	tbl.metadata, err = builder.Build()
	require.NoError(t, err)
}

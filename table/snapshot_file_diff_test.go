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
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeDiffTestManifest(t *testing.T, fs iceio.WriteFileIO, path string, id int64,
	content iceberg.ManifestContent, added, existing, deleted []iceberg.DataFile) iceberg.ManifestFile {
	t.Helper()
	var buf bytes.Buffer
	writer, err := iceberg.NewManifestWriter(2, &buf, *iceberg.UnpartitionedSpec,
		tableSchemaSimple, id, iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)
	for _, file := range added {
		require.NoError(t, writer.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &id, nil, nil, file)))
	}
	// Existing files were introduced by the previous commit. Their sequence
	// numbers must not change when a new manifest records them as EXISTING.
	originalID, originalSequence := int64(101), int64(1)
	for _, file := range existing {
		require.NoError(t, writer.Existing(iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING, &originalID, &originalSequence, &originalSequence, file)))
	}
	for _, file := range deleted {
		require.NoError(t, writer.Delete(iceberg.NewManifestEntry(
			iceberg.EntryStatusDELETED, &id, &originalSequence, &originalSequence, file)))
	}
	require.NoError(t, writer.Close())
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))
	manifest, err := writer.ToManifestFile(path, int64(buf.Len()), iceberg.WithManifestFileContent(content))
	require.NoError(t, err)
	return manifest
}

func makeDiffTestSnapshot(t *testing.T, fs iceio.WriteFileIO, dir string, id int64,
	dataAdded, dataExisting, dataDeleted, deletesAdded, deletesExisting, deletesDeleted []iceberg.DataFile) Snapshot {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "metadata"), 0o755))

	var manifests []iceberg.ManifestFile
	if len(dataAdded)+len(dataExisting)+len(dataDeleted) != 0 {
		manifests = append(manifests, writeDiffTestManifest(t, fs, dir+"/metadata/data.avro", id,
			iceberg.ManifestContentData, dataAdded, dataExisting, dataDeleted))
	}
	if len(deletesAdded)+len(deletesExisting)+len(deletesDeleted) != 0 {
		manifests = append(manifests, writeDiffTestManifest(t, fs, dir+"/metadata/deletes.avro", id,
			iceberg.ManifestContentDeletes, deletesAdded, deletesExisting, deletesDeleted))
	}
	path := dir + "/metadata/manifest-list.avro"
	out, err := fs.Create(context.Background(), path)
	require.NoError(t, err)
	sequence := id - 100
	require.NoError(t, iceberg.WriteManifestList(2, out, id, nil, &sequence, 0, manifests))
	require.NoError(t, out.Close())

	return Snapshot{
		SnapshotID:     id,
		SequenceNumber: sequence,
		TimestampMs:    time.Now().UnixMilli() + (id - 101),
		ManifestList:   path,
		Summary:        &Summary{Operation: OpAppend},
	}
}

func newDiffTestTable(t *testing.T, fsF FSysF, snapshots ...Snapshot) *Table {
	t.Helper()
	meta, err := NewMetadata(tableSchemaSimple, iceberg.UnpartitionedSpec,
		UnsortedSortOrder, filepath.ToSlash(t.TempDir()), nil)
	require.NoError(t, err)
	b, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	for i := range snapshots {
		require.NoError(t, b.AddSnapshot(&snapshots[i]))
	}
	if len(snapshots) > 0 {
		require.NoError(t, b.SetSnapshotRef(MainBranch, snapshots[len(snapshots)-1].SnapshotID, BranchRef))
	}
	meta, err = b.Build()
	require.NoError(t, err)
	return New(Identifier{"db", "snapshot_diff"}, meta, "", fsF, nil)
}

func diffFilePaths(files []iceberg.DataFile) []string {
	paths := make([]string, 0, len(files))
	for _, file := range files {
		paths = append(paths, file.FilePath())
	}
	return paths
}

func TestDiffSnapshotFiles(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())

	dataShared := buildSnapshotTestDataFile(t, dir+"/data/shared.parquet", iceberg.EntryContentData)
	dataGone := buildSnapshotTestDataFile(t, dir+"/data/gone.parquet", iceberg.EntryContentData)
	dataNew := buildSnapshotTestDataFile(t, dir+"/data/new.parquet", iceberg.EntryContentData)
	deleteShared := buildSnapshotTestDataFile(t, dir+"/deletes/shared.parquet", iceberg.EntryContentPosDeletes)
	deleteGone := buildSnapshotTestDataFile(t, dir+"/deletes/gone.parquet", iceberg.EntryContentEqDeletes)
	deleteNew := buildSnapshotTestDataFile(t, dir+"/deletes/new.parquet", iceberg.EntryContentPosDeletes)

	before := makeDiffTestSnapshot(t, fs, dir+"/before", 101,
		[]iceberg.DataFile{dataGone, dataShared}, nil, nil,
		[]iceberg.DataFile{deleteGone, deleteShared}, nil, nil)
	after := makeDiffTestSnapshot(t, fs, dir+"/after", 102,
		[]iceberg.DataFile{dataNew}, []iceberg.DataFile{dataShared}, []iceberg.DataFile{dataGone},
		[]iceberg.DataFile{deleteNew}, []iceberg.DataFile{deleteShared}, []iceberg.DataFile{deleteGone})
	after.Summary.Operation = OpOverwrite
	// These two snapshots have no parent relationship, like two diverged refs.
	tbl := newDiffTestTable(t, func(context.Context) (iceio.IO, error) { return fs, nil }, before, after)

	got, err := tbl.DiffSnapshotFiles(context.Background(), 101, 102)
	require.NoError(t, err)
	assert.Equal(t, []string{dataNew.FilePath()}, diffFilePaths(got.AddedDataFiles))
	assert.Equal(t, []string{dataGone.FilePath()}, diffFilePaths(got.RemovedDataFiles))
	assert.Equal(t, []string{deleteNew.FilePath()}, diffFilePaths(got.AddedDeleteFiles))
	assert.Equal(t, []string{deleteGone.FilePath()}, diffFilePaths(got.RemovedDeleteFiles))

	reverse, err := tbl.DiffSnapshotFiles(context.Background(), 102, 101)
	require.NoError(t, err)
	assert.Equal(t, diffFilePaths(got.RemovedDataFiles), diffFilePaths(reverse.AddedDataFiles))
	assert.Equal(t, diffFilePaths(got.AddedDataFiles), diffFilePaths(reverse.RemovedDataFiles))
	assert.Equal(t, diffFilePaths(got.RemovedDeleteFiles), diffFilePaths(reverse.AddedDeleteFiles))
	assert.Equal(t, diffFilePaths(got.AddedDeleteFiles), diffFilePaths(reverse.RemovedDeleteFiles))
}

func TestDiffSnapshotFilesDeterministicOrder(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	z := buildSnapshotTestDataFile(t, dir+"/data/z.parquet", iceberg.EntryContentData)
	a := buildSnapshotTestDataFile(t, dir+"/data/a.parquet", iceberg.EntryContentData)
	before := makeDiffTestSnapshot(t, fs, dir+"/before", 101, nil, nil, nil, nil, nil, nil)
	after := makeDiffTestSnapshot(t, fs, dir+"/after", 102,
		[]iceberg.DataFile{z, a}, nil, nil, nil, nil, nil)
	tbl := newDiffTestTable(t, func(context.Context) (iceio.IO, error) { return fs, nil }, before, after)

	got, err := tbl.DiffSnapshotFiles(context.Background(), 101, 102)
	require.NoError(t, err)
	assert.Equal(t, []string{a.FilePath(), z.FilePath()}, diffFilePaths(got.AddedDataFiles))
	assert.Empty(t, got.RemovedDataFiles)
	assert.Empty(t, got.AddedDeleteFiles)
	assert.Empty(t, got.RemovedDeleteFiles)
}

func TestDiffSnapshotFilesMissingAndSameSnapshot(t *testing.T) {
	first := Snapshot{SnapshotID: 101, SequenceNumber: 1, TimestampMs: time.Now().UnixMilli(),
		ManifestList: "/no-such-manifest.avro", Summary: &Summary{Operation: OpAppend}}
	calls := 0
	tbl := newDiffTestTable(t, func(context.Context) (iceio.IO, error) {
		calls++
		return nil, errors.New("unexpected IO")
	}, first)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 101, 101)
	require.NoError(t, err)
	assert.Empty(t, diff.AddedDataFiles)
	assert.Empty(t, diff.RemovedDataFiles)
	assert.Equal(t, 0, calls)

	_, err = tbl.DiffSnapshotFiles(context.Background(), 101, 999)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	assert.Equal(t, 0, calls)

	_, err = tbl.DiffSnapshotFiles(context.Background(), 999, 101)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	assert.Equal(t, 0, calls)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = tbl.DiffSnapshotFiles(ctx, 101, 101)
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, calls)
}

func TestDiffSnapshotFilesPropagatesManifestIOError(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	first := Snapshot{SnapshotID: 101, SequenceNumber: 1, TimestampMs: time.Now().UnixMilli(),
		ManifestList: dir + "/missing.avro", Summary: &Summary{Operation: OpAppend}}
	second := Snapshot{SnapshotID: 102, SequenceNumber: 2, TimestampMs: first.TimestampMs + 1,
		ManifestList: dir + "/missing2.avro", Summary: &Summary{Operation: OpAppend}}
	tbl := newDiffTestTable(t, func(context.Context) (iceio.IO, error) { return fs, nil }, first, second)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 101, 102)
	require.Error(t, err)
	require.ErrorContains(t, err, "snapshot 101")
	assert.Empty(t, diff.AddedDataFiles)
	assert.Empty(t, diff.RemovedDataFiles)
}

func TestDiffSnapshotFilesRejectsDuplicateLivePaths(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	duplicate := buildSnapshotTestDataFile(t, dir+"/data/duplicate.parquet", iceberg.EntryContentData)
	first := makeDiffTestSnapshot(t, fs, dir+"/before", 101, nil, nil, nil, nil, nil, nil)
	second := makeDiffTestSnapshot(t, fs, dir+"/after", 102,
		[]iceberg.DataFile{duplicate, duplicate}, nil, nil, nil, nil, nil)
	tbl := newDiffTestTable(t, func(context.Context) (iceio.IO, error) { return fs, nil }, first, second)

	_, err := tbl.DiffSnapshotFiles(context.Background(), 101, 102)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	require.ErrorContains(t, err, "more than once")
}

func TestDiffSnapshotFilesSamePathDifferentContent(t *testing.T) {
	fs := iceio.LocalFS{}
	dir := filepath.ToSlash(t.TempDir())
	path := dir + "/shared.parquet"
	data := buildSnapshotTestDataFile(t, path, iceberg.EntryContentData)
	deletes := buildSnapshotTestDataFile(t, path, iceberg.EntryContentPosDeletes)
	first := makeDiffTestSnapshot(t, fs, dir+"/before", 101,
		[]iceberg.DataFile{data}, nil, nil, nil, nil, nil)
	second := makeDiffTestSnapshot(t, fs, dir+"/after", 102,
		nil, nil, nil, []iceberg.DataFile{deletes}, nil, nil)
	tbl := newDiffTestTable(t, func(context.Context) (iceio.IO, error) { return fs, nil }, first, second)

	got, err := tbl.DiffSnapshotFiles(context.Background(), 101, 102)
	require.NoError(t, err)
	assert.Empty(t, got.AddedDataFiles)
	assert.Equal(t, []string{path}, diffFilePaths(got.RemovedDataFiles))
	assert.Equal(t, []string{path}, diffFilePaths(got.AddedDeleteFiles))
	assert.Empty(t, got.RemovedDeleteFiles)
}

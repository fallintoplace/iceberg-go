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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func snapshotDiffTable(fs iceio.IO, refs map[string]SnapshotRef, snapshots ...Snapshot) *Table {
	meta := &metadataV3{commonMetadata: commonMetadata{
		SnapshotList: snapshots,
		SnapshotRefs: refs,
	}}

	return New(Identifier{"test", "snapshot_diff"}, meta, "",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func snapshotDiffDataFile(t *testing.T, path string, content iceberg.ManifestEntryContent) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		content, path, iceberg.ParquetFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return builder.Build()
}

func snapshotDiffDeletionVector(t *testing.T, path, ref string, offset int64) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, path, iceberg.PuffinFile, nil, nil, nil, 10, 1024)
	require.NoError(t, err)

	return builder.ReferencedDataFile(ref).ContentOffset(offset).ContentSizeInBytes(100).Build()
}

func snapshotDiffManifest(t *testing.T, fs iceio.WriteFileIO, path string,
	content iceberg.ManifestContent, snapshotID int64,
	added, existing, deleted []iceberg.DataFile,
) iceberg.ManifestFile {
	t.Helper()

	var buf bytes.Buffer
	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	writer, err := iceberg.NewManifestWriter(3, &buf, *iceberg.UnpartitionedSpec,
		schema, snapshotID, iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)
	for _, file := range added {
		require.NoError(t, writer.Add(iceberg.NewManifestEntry(
			iceberg.EntryStatusADDED, &snapshotID, nil, nil, file)))
	}
	seq := int64(1)
	for _, file := range existing {
		require.NoError(t, writer.Existing(iceberg.NewManifestEntry(
			iceberg.EntryStatusEXISTING, &snapshotID, &seq, &seq, file)))
	}
	for _, file := range deleted {
		require.NoError(t, writer.Delete(iceberg.NewManifestEntry(
			iceberg.EntryStatusDELETED, &snapshotID, &seq, &seq, file)))
	}
	require.NoError(t, writer.Close())
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))

	manifest, err := writer.ToManifestFile(path, int64(buf.Len()),
		iceberg.WithManifestFileContent(content))
	require.NoError(t, err)

	return manifest
}

func snapshotDiffList(t *testing.T, fs iceio.WriteFileIO, snapshotID int64,
	path string, manifests ...iceberg.ManifestFile,
) Snapshot {
	t.Helper()
	out, err := fs.Create(context.Background(), path)
	require.NoError(t, err)
	seq := int64(1)
	require.NoError(t, iceberg.WriteManifestList(3, out, snapshotID, nil, &seq, 0, manifests))
	require.NoError(t, out.Close())

	return Snapshot{SnapshotID: snapshotID, SequenceNumber: seq, ManifestList: path}
}

func snapshotDiffPaths(files []iceberg.DataFile) []string {
	var paths []string
	for _, file := range files {
		paths = append(paths, file.FilePath())
	}

	return paths
}

type snapshotDiffDenyOpen struct {
	iceio.IO
	blocked string
}

func (fs snapshotDiffDenyOpen) Open(ctx context.Context, path string) (iceio.File, error) {
	if path == fs.blocked {
		return nil, fmt.Errorf("shared manifest %s should not be opened", path)
	}

	return fs.IO.Open(ctx, path)
}

func TestDiffSnapshotFilesRewrittenManifestsAndBranches(t *testing.T) {
	fs := iceio.NewMemFS()
	const root = "mem://snapshot-diff"
	oldA := snapshotDiffDataFile(t, root+"/old-a.parquet", iceberg.EntryContentData)
	oldZ := snapshotDiffDataFile(t, root+"/old-z.parquet", iceberg.EntryContentData)
	newA := snapshotDiffDataFile(t, root+"/new-a.parquet", iceberg.EntryContentData)
	newZ := snapshotDiffDataFile(t, root+"/new-z.parquet", iceberg.EntryContentData)
	carry := snapshotDiffDataFile(t, root+"/carry.parquet", iceberg.EntryContentData)
	keep := snapshotDiffDataFile(t, root+"/keep.parquet", iceberg.EntryContentData)
	tombstone := snapshotDiffDataFile(t, root+"/tombstone.parquet", iceberg.EntryContentData)
	oldDelete := snapshotDiffDataFile(t, root+"/old-delete.parquet", iceberg.EntryContentPosDeletes)
	newDelete := snapshotDiffDataFile(t, root+"/new-delete.parquet", iceberg.EntryContentEqDeletes)
	carryDelete := snapshotDiffDataFile(t, root+"/carry-delete.parquet", iceberg.EntryContentEqDeletes)

	sharedPath := root + "/shared.avro"
	shared := snapshotDiffManifest(t, fs, sharedPath, iceberg.ManifestContentData, 1,
		[]iceberg.DataFile{keep}, nil, nil)
	oldData := snapshotDiffManifest(t, fs, root+"/old-data.avro", iceberg.ManifestContentData, 1,
		[]iceberg.DataFile{oldZ, carry, oldA}, nil, []iceberg.DataFile{tombstone})
	newData := snapshotDiffManifest(t, fs, root+"/new-data.avro", iceberg.ManifestContentData, 2,
		[]iceberg.DataFile{newZ, newA}, []iceberg.DataFile{carry}, nil)
	oldDeletes := snapshotDiffManifest(t, fs, root+"/old-deletes.avro", iceberg.ManifestContentDeletes, 1,
		[]iceberg.DataFile{oldDelete, carryDelete}, nil, nil)
	newDeletes := snapshotDiffManifest(t, fs, root+"/new-deletes.avro", iceberg.ManifestContentDeletes, 2,
		[]iceberg.DataFile{newDelete}, []iceberg.DataFile{carryDelete}, nil)

	before := snapshotDiffList(t, fs, 1, root+"/before-list.avro", shared, oldData, oldDeletes)
	after := snapshotDiffList(t, fs, 2, root+"/after-list.avro", newDeletes, shared, newData)
	// The snapshots need not be parent and child: this also covers two
	// divergent branches. A shared immutable manifest is never read.
	tbl := snapshotDiffTable(snapshotDiffDenyOpen{IO: fs, blocked: sharedPath},
		map[string]SnapshotRef{
			"main":    {SnapshotID: 1, Type: BranchRef},
			"feature": {SnapshotID: 2, Type: BranchRef},
		}, before, after)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), diff.FromSnapshotID)
	assert.Equal(t, int64(2), diff.ToSnapshotID)
	assert.Equal(t, []string{newA.FilePath(), newZ.FilePath()}, snapshotDiffPaths(diff.AddedDataFiles))
	assert.Equal(t, []string{oldA.FilePath(), oldZ.FilePath()}, snapshotDiffPaths(diff.RemovedDataFiles))
	assert.Equal(t, []string{newDelete.FilePath()}, snapshotDiffPaths(diff.AddedDeleteFiles))
	assert.Equal(t, []string{oldDelete.FilePath()}, snapshotDiffPaths(diff.RemovedDeleteFiles))

	byRef, err := tbl.DiffSnapshotFilesByRef(context.Background(), "main", "feature")
	require.NoError(t, err)
	assert.Equal(t, diff, byRef)

	reverse, err := tbl.DiffSnapshotFiles(context.Background(), 2, 1)
	require.NoError(t, err)
	assert.Equal(t, snapshotDiffPaths(diff.RemovedDataFiles), snapshotDiffPaths(reverse.AddedDataFiles))
	assert.Equal(t, snapshotDiffPaths(diff.AddedDataFiles), snapshotDiffPaths(reverse.RemovedDataFiles))
	assert.Equal(t, snapshotDiffPaths(diff.RemovedDeleteFiles), snapshotDiffPaths(reverse.AddedDeleteFiles))
	assert.Equal(t, snapshotDiffPaths(diff.AddedDeleteFiles), snapshotDiffPaths(reverse.RemovedDeleteFiles))
}

func TestDiffSnapshotFilesDeletionVectorsSharePuffinPath(t *testing.T) {
	fs := iceio.NewMemFS()
	const root = "mem://snapshot-diff-dv"
	puffin := root + "/deletes.puffin"
	dvA := snapshotDiffDeletionVector(t, puffin, root+"/a.parquet", 10)
	dvBOld := snapshotDiffDeletionVector(t, puffin, root+"/b.parquet", 200)
	dvBNew := snapshotDiffDeletionVector(t, puffin, root+"/b.parquet", 300)

	oldManifest := snapshotDiffManifest(t, fs, root+"/old.avro", iceberg.ManifestContentDeletes, 1,
		[]iceberg.DataFile{dvA, dvBOld}, nil, nil)
	newManifest := snapshotDiffManifest(t, fs, root+"/new.avro", iceberg.ManifestContentDeletes, 2,
		[]iceberg.DataFile{dvA, dvBNew}, nil, nil)
	before := snapshotDiffList(t, fs, 1, root+"/old-list.avro", oldManifest)
	after := snapshotDiffList(t, fs, 2, root+"/new-list.avro", newManifest)
	tbl := snapshotDiffTable(fs, nil, before, after)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.NoError(t, err)
	require.Len(t, diff.RemovedDeleteFiles, 1)
	require.Len(t, diff.AddedDeleteFiles, 1)
	assert.Equal(t, int64(200), *diff.RemovedDeleteFiles[0].ContentOffset())
	assert.Equal(t, int64(300), *diff.AddedDeleteFiles[0].ContentOffset())
	assert.Empty(t, diff.AddedDataFiles)
	assert.Empty(t, diff.RemovedDataFiles)
}

func TestDiffSnapshotFilesSameSnapshotAndMissingIDs(t *testing.T) {
	tbl := snapshotDiffTable(nil, nil, Snapshot{SnapshotID: 11})
	diff, err := tbl.DiffSnapshotFiles(context.Background(), 11, 11)
	require.NoError(t, err)
	assert.Equal(t, SnapshotFileDiff{FromSnapshotID: 11, ToSnapshotID: 11}, diff)

	_, err = tbl.DiffSnapshotFiles(context.Background(), 11, 99)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = tbl.DiffSnapshotFiles(context.Background(), 99, 11)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = tbl.DiffSnapshotFilesByRef(context.Background(), "missing", "other")
	require.ErrorIs(t, err, ErrSnapshotNotFound)
}

func TestDiffSnapshotFilesCanceled(t *testing.T) {
	tbl := snapshotDiffTable(nil, nil, Snapshot{SnapshotID: 1}, Snapshot{SnapshotID: 2})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tbl.DiffSnapshotFiles(ctx, 1, 2)
	require.ErrorIs(t, err, context.Canceled)
	_, err = tbl.DiffSnapshotFilesByRef(ctx, "unused", "unused")
	require.ErrorIs(t, err, context.Canceled)
}

func TestDiffSnapshotFilesManifestReadFailureHasNoPartialResult(t *testing.T) {
	fs := iceio.NewMemFS()
	const root = "mem://snapshot-diff-errors"
	good := snapshotDiffDataFile(t, root+"/good.parquet", iceberg.EntryContentData)
	manifest := snapshotDiffManifest(t, fs, root+"/valid.avro", iceberg.ManifestContentData, 1,
		[]iceberg.DataFile{good}, nil, nil)
	missing := iceberg.NewManifestBuilder(3, root+"/not-present.avro", 100, 0, 2).
		Content(iceberg.ManifestContentData).Build()
	before := snapshotDiffList(t, fs, 1, root+"/before-list.avro", manifest)
	after := snapshotDiffList(t, fs, 2, root+"/after-list.avro", missing)
	tbl := snapshotDiffTable(fs, nil, before, after)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.Error(t, err)
	assert.Equal(t, SnapshotFileDiff{}, diff)
}

func TestDiffSnapshotFilesRejectsDuplicateLiveFiles(t *testing.T) {
	fs := iceio.NewMemFS()
	const root = "mem://snapshot-diff-duplicates"
	duplicated := snapshotDiffDataFile(t, root+"/same.parquet", iceberg.EntryContentData)
	old := snapshotDiffList(t, fs, 1, root+"/old-list.avro")
	one := snapshotDiffManifest(t, fs, root+"/one.avro", iceberg.ManifestContentData, 2,
		[]iceberg.DataFile{duplicated}, nil, nil)
	two := snapshotDiffManifest(t, fs, root+"/two.avro", iceberg.ManifestContentData, 2,
		[]iceberg.DataFile{duplicated}, nil, nil)
	newSnap := snapshotDiffList(t, fs, 2, root+"/new-list.avro", one, two)
	tbl := snapshotDiffTable(fs, nil, old, newSnap)

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.ErrorIs(t, err, ErrInvalidMetadata)
	assert.Equal(t, SnapshotFileDiff{}, diff)
}

func TestDiffSnapshotFilesRejectsInconsistentSharedManifest(t *testing.T) {
	fs := iceio.NewMemFS()
	const root = "mem://snapshot-diff-shared-errors"
	leftManifest := iceberg.NewManifestBuilder(3, root+"/same.avro", 100, 0, 1).
		Content(iceberg.ManifestContentData).Build()
	rightManifest := iceberg.NewManifestBuilder(3, root+"/same.avro", 100, 0, 1).
		Content(iceberg.ManifestContentDeletes).Build()
	before := snapshotDiffList(t, fs, 1, root+"/before.avro", leftManifest)
	after := snapshotDiffList(t, fs, 2, root+"/after.avro", rightManifest)
	tbl := snapshotDiffTable(fs, nil, before, after)

	_, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
	require.ErrorIs(t, err, ErrInvalidMetadata)
}

func TestDiffSnapshotFilesRejectsInvalidDeletionVector(t *testing.T) {
	invalid := snapshotDiffDataFile(t, "mem://invalid.parquet", iceberg.EntryContentData)
	_, err := snapshotDiffKey(invalid)
	require.NoError(t, err)
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec,
		iceberg.EntryContentPosDeletes, "mem://broken.puffin", iceberg.PuffinFile,
		nil, nil, nil, 1, 100)
	require.NoError(t, err)
	_, err = snapshotDiffKey(builder.ReferencedDataFile("mem://data.parquet").Build())
	require.True(t, errors.Is(err, ErrInvalidMetadata))
}

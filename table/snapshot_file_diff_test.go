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
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func snapshotDiffTestFile(t *testing.T, content iceberg.ManifestEntryContent, name string) iceberg.DataFile {
	t.Helper()
	builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, content,
		"mem://snapshot-diff/data/" + name, iceberg.ParquetFile, nil, nil, nil, 10, 100)
	require.NoError(t, err)
	return builder.Build()
}

func snapshotDiffTestEntry(status iceberg.ManifestEntryStatus, id int64, file iceberg.DataFile) iceberg.ManifestEntry {
	seq := int64(1)
	return iceberg.NewManifestEntry(status, &id, &seq, &seq, file)
}

func snapshotDiffWriteManifest(t *testing.T, fs iceio.WriteFileIO, id int64, name string,
	content iceberg.ManifestContent, entries ...iceberg.ManifestEntry,
) iceberg.ManifestFile {
	t.Helper()
	var buf bytes.Buffer
	writer, err := iceberg.NewManifestWriter(2, &buf, *iceberg.UnpartitionedSpec,
		simpleSchema(), id, iceberg.WithManifestWriterContent(content))
	require.NoError(t, err)
	for _, entry := range entries {
		switch entry.Status() {
		case iceberg.EntryStatusADDED:
			require.NoError(t, writer.Add(entry))
		case iceberg.EntryStatusEXISTING:
			require.NoError(t, writer.Existing(entry))
		case iceberg.EntryStatusDELETED:
			require.NoError(t, writer.Delete(entry))
		default:
			t.Fatalf("unexpected entry status %d", entry.Status())
		}
	}
	require.NoError(t, writer.Close())
	path := "mem://snapshot-diff/metadata/" + name + ".avro"
	manifest, err := writer.ToManifestFile(path, int64(buf.Len()))
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))
	return manifest
}

func snapshotDiffWriteList(t *testing.T, fs iceio.WriteFileIO, id, seq int64,
	name string, manifests ...iceberg.ManifestFile,
) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, iceberg.WriteManifestList(2, &buf, id, nil, &seq, 0, manifests))
	path := "mem://snapshot-diff/metadata/" + name + "-list.avro"
	require.NoError(t, fs.WriteFile(context.Background(), path, buf.Bytes()))
	return path
}

func snapshotDiffTestTable(t *testing.T, fs iceio.WriteFileIO, snapshots ...Snapshot) *Table {
	t.Helper()
	meta, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec,
		UnsortedSortOrder, "mem://snapshot-diff", nil)
	require.NoError(t, err)
	fsF := func(context.Context) (iceio.IO, error) { return fs, nil }
	txn := New(Identifier{"db", "snapshot-diff"}, meta, "metadata.json", fsF, nil).NewTransaction()
	txn.meta.snapshotList = snapshots
	currentID := snapshots[len(snapshots)-1].SnapshotID
	txn.meta.currentSnapshotID = &currentID
	txn.meta.refs = map[string]SnapshotRef{
		"left":     {SnapshotID: snapshots[0].SnapshotID, Type: BranchRef},
		"right":    {SnapshotID: currentID, Type: TagRef},
		MainBranch: {SnapshotID: currentID, Type: BranchRef},
	}
	built, err := txn.meta.Build()
	require.NoError(t, err)
	return New(Identifier{"db", "snapshot-diff"}, built, "metadata.json", fsF, nil)
}

func snapshotDiffPaths(files []iceberg.DataFile) []string {
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = file.FilePath()
	}
	return paths
}

func TestDiffSnapshotFilesDivergentSnapshots(t *testing.T) {
	fs := iceio.NewMemFS()
	oldFile := snapshotDiffTestFile(t, iceberg.EntryContentData, "old.parquet")
	sharedFile := snapshotDiffTestFile(t, iceberg.EntryContentData, "unchanged.parquet")
	newFileA := snapshotDiffTestFile(t, iceberg.EntryContentData, "a.parquet")
	newFileZ := snapshotDiffTestFile(t, iceberg.EntryContentData, "z.parquet")
	oldDelete := snapshotDiffTestFile(t, iceberg.EntryContentPosDeletes, "old-delete.parquet")
	newDelete := snapshotDiffTestFile(t, iceberg.EntryContentEqDeletes, "new-delete.parquet")
	tombstone := snapshotDiffTestFile(t, iceberg.EntryContentData, "already-gone.parquet")

	// This manifest exists in both lists but does not exist in storage. A
	// correct diff does not open it, because its live files cannot differ.
	commonManifest := iceberg.NewManifestBuilder(2,
		"mem://snapshot-diff/metadata/unreadable-common.avro", 123, 0, 10).
		SequenceNumber(1, 1).Content(iceberg.ManifestContentData).AddedFiles(1).Build()
	oldData := snapshotDiffWriteManifest(t, fs, 10, "old-data", iceberg.ManifestContentData,
		snapshotDiffTestEntry(iceberg.EntryStatusADDED, 10, oldFile),
		snapshotDiffTestEntry(iceberg.EntryStatusEXISTING, 10, sharedFile),
		snapshotDiffTestEntry(iceberg.EntryStatusDELETED, 10, tombstone))
	oldDeletes := snapshotDiffWriteManifest(t, fs, 10, "old-deletes", iceberg.ManifestContentDeletes,
		snapshotDiffTestEntry(iceberg.EntryStatusADDED, 10, oldDelete))
	newData := snapshotDiffWriteManifest(t, fs, 20, "new-data", iceberg.ManifestContentData,
		snapshotDiffTestEntry(iceberg.EntryStatusADDED, 20, newFileZ),
		snapshotDiffTestEntry(iceberg.EntryStatusADDED, 20, newFileA),
		snapshotDiffTestEntry(iceberg.EntryStatusEXISTING, 10, sharedFile))
	newDeletes := snapshotDiffWriteManifest(t, fs, 20, "new-deletes", iceberg.ManifestContentDeletes,
		snapshotDiffTestEntry(iceberg.EntryStatusADDED, 20, newDelete))

	oldList := snapshotDiffWriteList(t, fs, 10, 1, "old", commonManifest, oldData, oldDeletes)
	newList := snapshotDiffWriteList(t, fs, 20, 2, "new", commonManifest, newData, newDeletes)
	tbl := snapshotDiffTestTable(t, fs,
		Snapshot{SnapshotID: 10, SequenceNumber: 1, ManifestList: oldList},
		Snapshot{SnapshotID: 20, SequenceNumber: 2, ManifestList: newList})

	diff, err := tbl.DiffSnapshotFiles(context.Background(), 10, 20)
	require.NoError(t, err)
	require.Equal(t, int64(10), diff.FromSnapshotID)
	require.Equal(t, int64(20), diff.ToSnapshotID)
	require.Equal(t, []string{"mem://snapshot-diff/data/a.parquet", "mem://snapshot-diff/data/z.parquet"},
		snapshotDiffPaths(diff.AddedDataFiles))
	require.Equal(t, []string{oldFile.FilePath()}, snapshotDiffPaths(diff.RemovedDataFiles))
	require.Equal(t, []string{newDelete.FilePath()}, snapshotDiffPaths(diff.AddedDeleteFiles))
	require.Equal(t, []string{oldDelete.FilePath()}, snapshotDiffPaths(diff.RemovedDeleteFiles))

	reverse, err := tbl.DiffSnapshotFiles(context.Background(), 20, 10)
	require.NoError(t, err)
	require.Equal(t, snapshotDiffPaths(diff.AddedDataFiles), snapshotDiffPaths(reverse.RemovedDataFiles))
	require.Equal(t, snapshotDiffPaths(diff.RemovedDataFiles), snapshotDiffPaths(reverse.AddedDataFiles))
	require.Equal(t, snapshotDiffPaths(diff.AddedDeleteFiles), snapshotDiffPaths(reverse.RemovedDeleteFiles))
	require.Equal(t, snapshotDiffPaths(diff.RemovedDeleteFiles), snapshotDiffPaths(reverse.AddedDeleteFiles))

	refs, err := tbl.DiffSnapshotRefs(context.Background(), "left", "right")
	require.NoError(t, err)
	require.Equal(t, snapshotDiffPaths(diff.AddedDataFiles), snapshotDiffPaths(refs.AddedDataFiles))
	require.Equal(t, snapshotDiffPaths(diff.RemovedDeleteFiles), snapshotDiffPaths(refs.RemovedDeleteFiles))
}

func TestDiffSnapshotFilesNoIOForIdenticalSnapshots(t *testing.T) {
	tbl := snapshotDiffTestTable(t, iceio.NewMemFS(),
		Snapshot{SnapshotID: 7, ManifestList: "mem://missing-list.avro"})
	diff, err := tbl.DiffSnapshotFiles(context.Background(), 7, 7)
	require.NoError(t, err)
	require.Equal(t, int64(7), diff.FromSnapshotID)
	require.Empty(t, diff.AddedDataFiles)
	require.Empty(t, diff.RemovedDataFiles)
	require.Empty(t, diff.AddedDeleteFiles)
	require.Empty(t, diff.RemovedDeleteFiles)
}

func TestDiffSnapshotFilesRejectMissingSnapshotOrRef(t *testing.T) {
	tbl := snapshotDiffTestTable(t, iceio.NewMemFS(), Snapshot{SnapshotID: 7})
	_, err := tbl.DiffSnapshotFiles(context.Background(), 8, 7)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = tbl.DiffSnapshotFiles(context.Background(), 7, 8)
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = tbl.DiffSnapshotRefs(context.Background(), "missing", "right")
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = tbl.DiffSnapshotRefs(context.Background(), "left", "missing")
	require.ErrorIs(t, err, ErrSnapshotNotFound)
	_, err = (Table{}).DiffSnapshotFiles(context.Background(), 1, 2)
	require.ErrorIs(t, err, ErrInvalidOperation)
}

func TestDiffSnapshotFilesCancellationAndReadFailure(t *testing.T) {
	fs := iceio.NewMemFS()
	manifest := iceberg.NewManifestBuilder(2,
		"mem://snapshot-diff/metadata/missing.avro", 20, 0, 10).
		SequenceNumber(1, 1).Content(iceberg.ManifestContentData).AddedFiles(1).Build()
	oldList := snapshotDiffWriteList(t, fs, 10, 1, "broken-old", manifest)
	newList := snapshotDiffWriteList(t, fs, 20, 2, "empty-new")
	tbl := snapshotDiffTestTable(t, fs,
		Snapshot{SnapshotID: 10, SequenceNumber: 1, ManifestList: oldList},
		Snapshot{SnapshotID: 20, SequenceNumber: 2, ManifestList: newList})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tbl.DiffSnapshotFiles(ctx, 10, 20)
	require.ErrorIs(t, err, context.Canceled)
	_, err = tbl.DiffSnapshotRefs(ctx, "left", "right")
	require.True(t, errors.Is(err, context.Canceled))

	_, err = tbl.DiffSnapshotFiles(context.Background(), 10, 20)
	require.ErrorContains(t, err, "missing.avro")
	require.ErrorContains(t, err, "source snapshot 10")
}

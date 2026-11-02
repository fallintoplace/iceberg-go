// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to You under the Apache License, Version 2.0 (the
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
    "github.com/stretchr/testify/require"
)

type diffEntrySpec struct {
    path string
    status iceberg.ManifestEntryStatus
}

func writeV1DiffManifest(t *testing.T, fs *trackingCallsIO, snapshotID int64, path string, files ...diffEntrySpec) iceberg.ManifestFile {
    t.Helper()
    var entries []iceberg.ManifestEntry
    for _, spec := range files {
        df, err := iceberg.NewDataFileBuilder(
            *iceberg.UnpartitionedSpec, iceberg.EntryContentData, spec.path,
            iceberg.ParquetFile, nil, nil, nil, 10, 1024)
        require.NoError(t, err)
        entries = append(entries, iceberg.NewManifestEntryBuilder(spec.status, &snapshotID, df.Build()).Build())
    }
    var out bytes.Buffer
    manifest, err := iceberg.WriteManifest(
        path, &out, 1, *iceberg.UnpartitionedSpec, simpleSchema(), snapshotID, entries)
    require.NoError(t, err)
    require.NoError(t, fs.WriteFile(context.Background(), path, out.Bytes()))
    return manifest
}

type diffSnapshotSpec struct {
    id int64
    manifests []iceberg.ManifestFile
    embedded bool
}

func makeV1DiffTable(t *testing.T, fs *trackingCallsIO, snapshots ...diffSnapshotSpec) *Table {
    t.Helper()
    base, err := NewMetadata(simpleSchema(), iceberg.UnpartitionedSpec, UnsortedSortOrder,
        "mem://snapshot-diff", iceberg.Properties{PropertyFormatVersion: "1"})
    require.NoError(t, err)
    builder, err := MetadataBuilderFromBase(base, "")
    require.NoError(t, err)

    for _, s := range snapshots {
        snapshot := &Snapshot{
            SnapshotID: s.id,
            TimestampMs: base.LastUpdatedMillis() + s.id,
        }
        if s.embedded {
            snapshot.ManifestLocations = make([]string, 0, len(s.manifests))
            for _, m := range s.manifests {
                snapshot.ManifestLocations = append(snapshot.ManifestLocations, m.FilePath())
            }
        } else {
            var out bytes.Buffer
            require.NoError(t, iceberg.WriteManifestList(
                1, &out, s.id, nil, nil, 0, s.manifests))
            snapshot.ManifestList = fmt.Sprintf("mem://snapshot-diff/metadata/%d.avro", s.id)
            require.NoError(t, fs.WriteFile(context.Background(), snapshot.ManifestList, out.Bytes()))
        }
        require.NoError(t, builder.AddSnapshot(snapshot))
    }

    require.NotEmpty(t, snapshots)
    require.NoError(t, builder.SetSnapshotRef(MainBranch, snapshots[len(snapshots)-1].id, BranchRef))
    meta, err := builder.Build()
    require.NoError(t, err)
    return New(Identifier{"ns", "snapshot_diff"}, meta, "", testFSF(fs), nil)
}

func diffFilePaths(files []iceberg.DataFile) []string {
    result := make([]string, 0, len(files))
    for _, file := range files {
        result = append(result, file.FilePath())
    }
    return result
}

func TestDiffSnapshotFilesV1SharedAndRewrittenManifests(t *testing.T) {
    fs := newTrackingCallsIO()
    shared := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/shared.avro",
        diffEntrySpec{path: "mem://snapshot-diff/shared.parquet", status: iceberg.EntryStatusADDED})
    before := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/before.avro",
        diffEntrySpec{path: "mem://snapshot-diff/keep.parquet", status: iceberg.EntryStatusADDED},
        diffEntrySpec{path: "mem://snapshot-diff/removed.parquet", status: iceberg.EntryStatusADDED})
    after := writeV1DiffManifest(t, fs, 2, "mem://snapshot-diff/after.avro",
        diffEntrySpec{path: "mem://snapshot-diff/keep.parquet", status: iceberg.EntryStatusEXISTING},
        diffEntrySpec{path: "mem://snapshot-diff/added.parquet", status: iceberg.EntryStatusADDED},
        diffEntrySpec{path: "mem://snapshot-diff/removed.parquet", status: iceberg.EntryStatusDELETED})
    table := makeV1DiffTable(t, fs,
        diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{shared, before}},
        diffSnapshotSpec{id: 2, manifests: []iceberg.ManifestFile{shared, after}})

    diff, err := table.DiffSnapshotFiles(context.Background(), 1, 2)
    require.NoError(t, err)
    require.False(t, diff.Empty())
    require.Equal(t, int64(1), diff.FromSnapshotID)
    require.Equal(t, int64(2), diff.ToSnapshotID)
    require.Equal(t, []string{"mem://snapshot-diff/added.parquet"}, diffFilePaths(diff.AddedFiles))
    require.Equal(t, []string{"mem://snapshot-diff/removed.parquet"}, diffFilePaths(diff.RemovedFiles))
    require.Zero(t, fs.openCount[shared.FilePath()], "unchanged manifests should not be decoded")
    require.Equal(t, 1, fs.openCount[before.FilePath()])
    require.Equal(t, 1, fs.openCount[after.FilePath()])

    reverse, err := table.DiffSnapshotFiles(context.Background(), 2, 1)
    require.NoError(t, err)
    require.Equal(t, diffFilePaths(diff.AddedFiles), diffFilePaths(reverse.RemovedFiles))
    require.Equal(t, diffFilePaths(diff.RemovedFiles), diffFilePaths(reverse.AddedFiles))
}

func TestDiffSnapshotFilesV1ManifestRewriteNoFileChanges(t *testing.T) {
    fs := newTrackingCallsIO()
    a := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/manifest-a.avro",
        diffEntrySpec{path: "mem://snapshot-diff/x.parquet", status: iceberg.EntryStatusADDED})
    b := writeV1DiffManifest(t, fs, 2, "mem://snapshot-diff/manifest-b.avro",
        diffEntrySpec{path: "mem://snapshot-diff/x.parquet", status: iceberg.EntryStatusEXISTING})
    tbl := makeV1DiffTable(t, fs,
        diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{a}},
        diffSnapshotSpec{id: 2, manifests: []iceberg.ManifestFile{b}})

    diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
    require.NoError(t, err)
    require.True(t, diff.Empty())
    require.Empty(t, diff.AddedFiles)
    require.Empty(t, diff.RemovedFiles)
}

func TestDiffSnapshotFilesV1EmbeddedManifestsAndUnrelatedSnapshots(t *testing.T) {
    fs := newTrackingCallsIO()
    a := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/legacy-a.avro",
        diffEntrySpec{path: "mem://snapshot-diff/a.parquet", status: iceberg.EntryStatusADDED})
    b := writeV1DiffManifest(t, fs, 2, "mem://snapshot-diff/legacy-b.avro",
        diffEntrySpec{path: "mem://snapshot-diff/b.parquet", status: iceberg.EntryStatusADDED})
    tbl := makeV1DiffTable(t, fs,
        diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{a}, embedded: true},
        diffSnapshotSpec{id: 2, manifests: []iceberg.ManifestFile{b}, embedded: true})
    // Neither snapshot has a parent, and neither is an ancestor of the other.
    diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
    require.NoError(t, err)
    require.Equal(t, []string{"mem://snapshot-diff/b.parquet"}, diffFilePaths(diff.AddedFiles))
    require.Equal(t, []string{"mem://snapshot-diff/a.parquet"}, diffFilePaths(diff.RemovedFiles))
}

func TestDiffSnapshotFilesSkipsIdenticalSnapshotWithoutIO(t *testing.T) {
    fs := newTrackingCallsIO()
    manifest := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/same.avro",
        diffEntrySpec{path: "mem://snapshot-diff/a.parquet", status: iceberg.EntryStatusADDED})
    tbl := makeV1DiffTable(t, fs, diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{manifest}})
    diff, err := tbl.DiffSnapshotFiles(context.Background(), 1, 1)
    require.NoError(t, err)
    require.True(t, diff.Empty())
    require.Empty(t, fs.openCount)
}

func TestDiffSnapshotFilesErrors(t *testing.T) {
    fs := newTrackingCallsIO()
    manifest := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/good.avro",
        diffEntrySpec{path: "mem://snapshot-diff/a.parquet", status: iceberg.EntryStatusADDED})
    missing := iceberg.NewManifestBuilder(1, "mem://snapshot-diff/missing.avro", 99, 0, 2).
        AddedFiles(1).Build()
    tbl := makeV1DiffTable(t, fs,
        diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{manifest}},
        diffSnapshotSpec{id: 2, manifests: []iceberg.ManifestFile{missing}})

    _, err := tbl.DiffSnapshotFiles(context.Background(), 1, 99)
    require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
    _, err = tbl.DiffSnapshotFiles(context.Background(), 99, 2)
    require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
    _, err = tbl.DiffSnapshotFiles(nil, 1, 2)
    require.ErrorIs(t, err, iceberg.ErrInvalidArgument)

    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    _, err = tbl.DiffSnapshotFiles(ctx, 1, 2)
    require.ErrorIs(t, err, context.Canceled)

    _, err = tbl.DiffSnapshotFiles(context.Background(), 1, 2)
    require.Error(t, err)
    require.ErrorContains(t, err, "missing.avro")
}

func TestDiffSnapshotFilesRejectsDuplicateManifest(t *testing.T) {
    fs := newTrackingCallsIO()
    manifest := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/repeated.avro",
        diffEntrySpec{path: "mem://snapshot-diff/a.parquet", status: iceberg.EntryStatusADDED})
    tbl := makeV1DiffTable(t, fs,
        diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{manifest, manifest}},
        diffSnapshotSpec{id: 2, manifests: nil})
    _, err := tbl.DiffSnapshotFiles(context.Background(), 1, 2)
    require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
    require.ErrorContains(t, err, "duplicate manifest")
}

func TestDiffSnapshotFilesContextPreserved(t *testing.T) {
    fs := newTrackingCallsIO()
    manifest := writeV1DiffManifest(t, fs, 1, "mem://snapshot-diff/a.avro",
        diffEntrySpec{path: "mem://snapshot-diff/a.parquet", status: iceberg.EntryStatusADDED})
    tbl := makeV1DiffTable(t, fs,
        diffSnapshotSpec{id: 1, manifests: []iceberg.ManifestFile{manifest}},
        diffSnapshotSpec{id: 2, manifests: nil})
    ctx, cancel := context.WithCancel(context.Background())
    cancel()
    _, err := tbl.DiffSnapshotFiles(ctx, 1, 2)
    require.True(t, errors.Is(err, context.Canceled))
}

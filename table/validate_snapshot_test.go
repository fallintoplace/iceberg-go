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
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const snapshotValidationTestID int64 = 41

func v1ValidationEntry(t *testing.T, status iceberg.ManifestEntryStatus, path string) iceberg.ManifestEntry {
	t.Helper()

	data := newTestDataFile(t, iceberg.NewPartitionSpec(), path, nil)
	snapshotID := snapshotValidationTestID

	return iceberg.NewManifestEntry(status, &snapshotID, nil, nil, data)
}

// v1SnapshotValidationTable builds actual v1 Avro manifests and a v1 manifest
// list. No data files are written because the audit only reads metadata.
func v1SnapshotValidationTable(
	t *testing.T,
	embedded bool,
	modify func([]iceberg.ManifestFile) []iceberg.ManifestFile,
	groups ...[]iceberg.ManifestEntry,
) *Table {
	t.Helper()

	ctx := context.Background()
	spec := iceberg.NewPartitionSpec()
	txn, fs := createTestTransactionWithMemIO(t, spec)
	txn.meta.formatVersion = 1

	manifests := make([]iceberg.ManifestFile, 0, len(groups))
	for i, entries := range groups {
		path := fmt.Sprintf("mem://default/table-location/metadata/validate-%d.avro", i)
		var buf bytes.Buffer
		manifest, err := iceberg.WriteManifest(path, &buf, 1, spec, simpleSchema(), snapshotValidationTestID, entries)
		require.NoError(t, err)
		require.NoError(t, fs.WriteFile(ctx, path, buf.Bytes()))
		manifests = append(manifests, manifest)
	}
	if modify != nil {
		manifests = modify(manifests)
	}

	snapshot := Snapshot{
		SnapshotID:  snapshotValidationTestID,
		TimestampMs: time.Now().UnixMilli(),
		Summary:     &Summary{Operation: OpAppend},
	}
	if embedded {
		for _, mf := range manifests {
			snapshot.ManifestLocations = append(snapshot.ManifestLocations, mf.FilePath())
		}
	} else {
		const listPath = "mem://default/table-location/metadata/validation-list.avro"
		var listBuf bytes.Buffer
		require.NoError(t, iceberg.WriteManifestList(1, &listBuf, snapshotValidationTestID,
			nil, nil, 0, manifests))
		require.NoError(t, fs.WriteFile(ctx, listPath, listBuf.Bytes()))
		snapshot.ManifestList = listPath
	}

	require.NoError(t, txn.meta.AddSnapshot(&snapshot))
	require.NoError(t, txn.meta.SetSnapshotRef(MainBranch, snapshotValidationTestID, BranchRef))
	meta, err := txn.meta.Build()
	require.NoError(t, err)

	return New(Identifier{"db", "audit"}, meta, "metadata.json",
		func(context.Context) (iceio.IO, error) { return fs, nil }, nil)
}

func TestValidateSnapshotV1UniqueLiveFiles(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://first.parquet"),
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusEXISTING, "file://second.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	require.NotNil(t, report)
	assert.True(t, report.Valid())
	assert.True(t, report.Complete)
	assert.Equal(t, snapshotValidationTestID, report.SnapshotID)
	assert.Equal(t, int64(2), report.ManifestsChecked)
	assert.Equal(t, int64(2), report.LiveFilesChecked)
	assert.Empty(t, report.Issues)
}

func TestValidateSnapshotV1DuplicateLiveFileAcrossManifests(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusEXISTING, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.False(t, report.Valid())
	assert.True(t, report.Complete)
	assert.Equal(t, int64(2), report.LiveFilesChecked)
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[0].Code)
	assert.Equal(t, "file://same.parquet", report.Issues[0].FilePath)
	assert.Contains(t, report.Issues[0].FirstManifestPath, "validate-0.avro")
	assert.Contains(t, report.Issues[0].ManifestPath, "validate-1.avro")
}

func TestValidateSnapshotV1DuplicateLiveFileWithinManifest(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
			v1ValidationEntry(t, iceberg.EntryStatusEXISTING, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.False(t, report.Valid())
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[0].Code)
	assert.Equal(t, report.Issues[0].FirstManifestPath, report.Issues[0].ManifestPath)
}

func TestValidateSnapshotV1IgnoresDeletedEntriesForUniqueness(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusDELETED, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Valid())
	assert.Equal(t, int64(1), report.LiveFilesChecked)
}

func TestValidateSnapshotV1EmbeddedManifests(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, true, nil,
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://embedded.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Valid())
	assert.Equal(t, int64(1), report.ManifestsChecked)
	assert.Equal(t, int64(1), report.LiveFilesChecked)
}

func TestValidateSnapshotV1RepeatedManifest(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false,
		func(manifests []iceberg.ManifestFile) []iceberg.ManifestFile {
			return append(manifests, manifests[0])
		},
		[]iceberg.ManifestEntry{
			v1ValidationEntry(t, iceberg.EntryStatusADDED, "file://same.parquet"),
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Complete)
	assert.False(t, report.Valid())
	assert.Equal(t, int64(2), report.ManifestsChecked)
	require.Len(t, report.Issues, 2)
	assert.Equal(t, SnapshotIssueDuplicateManifest, report.Issues[0].Code)
	assert.Equal(t, SnapshotIssueDuplicateLiveFile, report.Issues[1].Code)
}

func TestValidateSnapshotV1EmptyManifestList(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.NoError(t, err)
	assert.True(t, report.Valid())
	assert.Zero(t, report.ManifestsChecked)
	assert.Zero(t, report.LiveFilesChecked)
}

func TestValidateSnapshotV1MissingManifestReturnsIncompleteReport(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false,
		func(_ []iceberg.ManifestFile) []iceberg.ManifestFile {
			return []iceberg.ManifestFile{
				iceberg.NewManifestBuilder(1,
					"mem://default/table-location/metadata/missing.avro", 123, 0,
					snapshotValidationTestID).AddedFiles(1).Build(),
			}
		},
	)

	report, err := tbl.ValidateSnapshot(context.Background(), snapshotValidationTestID)
	require.Error(t, err)
	require.NotNil(t, report)
	assert.ErrorContains(t, err, "missing.avro")
	assert.False(t, report.Complete)
	assert.False(t, report.Valid())
	assert.Equal(t, int64(1), report.ManifestsChecked)
}

func TestValidateSnapshotCanceledContextReturnsIncompleteReport(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	report, err := tbl.ValidateSnapshot(ctx, snapshotValidationTestID)
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, report)
	assert.False(t, report.Complete)
	assert.False(t, report.Valid())
	assert.Zero(t, report.ManifestsChecked)
}

func TestValidateSnapshotRejectsUnknownSnapshot(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)

	report, err := tbl.ValidateSnapshot(context.Background(), 999)
	require.Error(t, err)
	assert.Nil(t, report)
	assert.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestValidateSnapshotRejectsNilContext(t *testing.T) {
	tbl := v1SnapshotValidationTable(t, false, nil)

	report, err := tbl.ValidateSnapshot(nil, snapshotValidationTestID)
	require.Error(t, err)
	assert.Nil(t, report)
	assert.True(t, errors.Is(err, iceberg.ErrInvalidArgument))
}

func TestValidateSnapshotV1RejectsIncompatibleManifestDescriptor(t *testing.T) {
	manifest := iceberg.NewManifestBuilder(2, "file://manifest.avro", 123, 99,
		snapshotValidationTestID).
		Content(iceberg.ManifestContentDeletes).
		Build()
	report := &SnapshotValidationReport{}
	validateSnapshotManifestDescriptor(report, manifest, 1, map[int]struct{}{0: {}})

	require.Len(t, report.Issues, 3)
	assert.Equal(t, SnapshotIssueUnknownPartitionSpec, report.Issues[0].Code)
	assert.Equal(t, int32(99), report.Issues[0].PartitionSpecID)
	assert.Equal(t, SnapshotIssueIncompatibleManifestVersion, report.Issues[1].Code)
	assert.Equal(t, SnapshotIssueInvalidManifestContent, report.Issues[2].Code)
}

func TestValidateManifestEntryCountOptionalV1(t *testing.T) {
	report := &SnapshotValidationReport{}
	validateManifestEntryCount(report, "manifest", "added", -1, 2)
	validateManifestEntryCount(report, "manifest", "added", 2, 2)
	assert.Empty(t, report.Issues)

	validateManifestEntryCount(report, "manifest", "added", 3, 2)
	require.Len(t, report.Issues, 1)
	assert.Equal(t, SnapshotIssueManifestCountMismatch, report.Issues[0].Code)
	assert.Contains(t, report.Issues[0].Detail, "reports 3, found 2")
}

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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func fastForwardTestMetadata(t *testing.T) Metadata {
	t.Helper()

	schema := iceberg.NewSchema(0, iceberg.NestedField{
		ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
	})
	props := iceberg.Properties{PropertyFormatVersion: "2"}
	for key, value := range assertRefRetryProps {
		props[key] = value
	}
	base, err := NewMetadata(schema, iceberg.UnpartitionedSpec,
		UnsortedSortOrder, t.TempDir(), props)
	require.NoError(t, err)

	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	root, child := int64(100), int64(200)
	snapshots := []Snapshot{
		{SnapshotID: root, SequenceNumber: 1, TimestampMs: base.LastUpdatedMillis() + 1,
			Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: child, ParentSnapshotID: &root, SequenceNumber: 2,
			TimestampMs: base.LastUpdatedMillis() + 2, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 300, ParentSnapshotID: &child, SequenceNumber: 3,
			TimestampMs: base.LastUpdatedMillis() + 3, Summary: &Summary{Operation: OpAppend}},
		{SnapshotID: 400, ParentSnapshotID: &root, SequenceNumber: 4,
			TimestampMs: base.LastUpdatedMillis() + 4, Summary: &Summary{Operation: OpAppend}},
	}
	for i := range snapshots {
		require.NoError(t, builder.AddSnapshot(&snapshots[i]))
	}

	require.NoError(t, builder.SetSnapshotRef(MainBranch, 100, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("feature", 300, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("target", 200, BranchRef,
		WithMaxRefAgeMs(86400000), WithMaxSnapshotAgeMs(3600000),
		WithMinSnapshotsToKeep(3)))
	require.NoError(t, builder.SetSnapshotRef("audit", 300, TagRef, WithMaxRefAgeMs(900000)))
	require.NoError(t, builder.SetSnapshotRef("diverged", 400, BranchRef))
	meta, err := builder.Build()
	require.NoError(t, err)
	return meta
}

func fastForwardTestRef(t *testing.T, meta Metadata, name string) SnapshotRef {
	t.Helper()
	for refName, ref := range meta.Refs() {
		if name == refName {
			return ref
		}
	}
	t.Fatalf("missing ref %q", name)
	return SnapshotRef{}
}

func moveFastForwardTestRef(t *testing.T, base Metadata, name string, snapshotID int64) Metadata {
	t.Helper()
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	prev := fastForwardTestRef(t, base, name).SnapshotID
	seq := int64(0)
	for _, snap := range base.Snapshots() {
		if snap.SequenceNumber > seq {
			seq = snap.SequenceNumber
		}
	}
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: snapshotID, ParentSnapshotID: &prev, SequenceNumber: seq + 1,
		TimestampMs: base.LastUpdatedMillis() + 1,
		Summary: &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef(name, snapshotID, BranchRef))
	updated, err := builder.Build()
	require.NoError(t, err)
	return updated
}

func TestFastForwardBranchPreservesRetentionAndSnapshots(t *testing.T) {
	base := fastForwardTestMetadata(t)
	original := fastForwardTestRef(t, base, "target")
	tbl, cat := newAssertRefTestTable(t, base, base)
	tx, err := tbl.NewTransactionOnBranch("target")
	require.NoError(t, err)
	require.NoError(t, tx.FastForwardBranch("target", "feature"))

	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Len(t, commit.Updates, 1, "fast-forward must not add snapshots")

	result, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.NotNil(t, result)

	got := fastForwardTestRef(t, cat.metadata, "target")
	assert.Equal(t, int64(300), got.SnapshotID)
	assert.Equal(t, original.MaxRefAgeMs, got.MaxRefAgeMs)
	assert.Equal(t, original.MaxSnapshotAgeMs, got.MaxSnapshotAgeMs)
	assert.Equal(t, original.MinSnapshotsToKeep, got.MinSnapshotsToKeep)
	assert.Equal(t, int64(300), fastForwardTestRef(t, cat.metadata, "feature").SnapshotID)
	assert.Len(t, cat.metadata.Snapshots(), len(base.Snapshots()))
}

func TestFastForwardBranchFromTagAndCreateMissingBranch(t *testing.T) {
	for _, tc := range []struct {
		name, from, to string
	}{
		{"tag source", "target", "audit"},
		{"create missing branch", "new-feature", "feature"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fastForwardTestMetadata(t)
			tbl, cat := newAssertRefTestTable(t, base, base)
			tx, err := tbl.NewTransactionOnBranch(tc.from)
			require.NoError(t, err)
			require.NoError(t, tx.FastForwardBranch(tc.from, tc.to))
			_, err = tx.Commit(t.Context())
			require.NoError(t, err)

			got := fastForwardTestRef(t, cat.metadata, tc.from)
			assert.Equal(t, BranchRef, got.Type)
			assert.Equal(t, int64(300), got.SnapshotID)
			if tc.from == "new-feature" {
				assert.Nil(t, got.MaxRefAgeMs)
				assert.Nil(t, got.MaxSnapshotAgeMs)
				assert.Nil(t, got.MinSnapshotsToKeep)
			}
		})
	}
}

func TestFastForwardBranchMainAndConvenienceAPI(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)
	result, err := tbl.FastForwardBranch(t.Context(), MainBranch, "feature")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, int64(300), result.CurrentSnapshot().SnapshotID)
	assert.Equal(t, int64(300), fastForwardTestRef(t, cat.metadata, MainBranch).SnapshotID)
	assert.Len(t, cat.metadata.Snapshots(), len(base.Snapshots()))
}

func TestFastForwardBranchRejectsInvalidReferences(t *testing.T) {
	for _, tc := range []struct {
		name, from, to, message string
	}{
		{"empty destination", "", "feature", "non-empty"},
		{"empty source", "target", "", "non-empty"},
		{"unknown source", "target", "unknown", "does not exist"},
		{"destination is a tag", "audit", "feature", "not a branch"},
		{"divergent ancestry", "diverged", "feature", "not an ancestor"},
		{"backward move", "feature", "target", "not an ancestor"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fastForwardTestMetadata(t)
			tbl, _ := newAssertRefTestTable(t, base, base)
			tx := tbl.NewTransaction()
			err := tx.FastForwardBranch(tc.from, tc.to)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			assert.ErrorContains(t, err, tc.message)
			commit, err := tx.TableCommit()
			require.NoError(t, err)
			assert.Empty(t, commit.Updates, "failed validation must not stage updates")
		})
	}
}

func TestFastForwardBranchNoopAndTransactionState(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)
	tx, err := tbl.NewTransactionOnBranch("target")
	require.NoError(t, err)
	require.NoError(t, tx.FastForwardBranch("target", "target"))
	commit, err := tx.TableCommit()
	require.NoError(t, err)
	require.Empty(t, commit.Updates)
	_, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Zero(t, cat.attempts.Load(), "no-op should not commit metadata")
	assert.ErrorContains(t, tx.FastForwardBranch("target", "target"), "already been committed")

	var nilTxn *Transaction
	assert.Error(t, nilTxn.FastForwardBranch("target", "feature"))
}

func TestFastForwardBranchRejectsConcurrentChanges(t *testing.T) {
	for _, tc := range []struct {
		name, ref string
	}{
		{"source moved", "feature"},
		{"destination moved", "target"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fastForwardTestMetadata(t)
			concurrent := moveFastForwardTestRef(t, base, tc.ref, 500)
			tbl, cat := newAssertRefTestTable(t, base, concurrent)
			tx, err := tbl.NewTransactionOnBranch("target")
			require.NoError(t, err)
			require.NoError(t, tx.FastForwardBranch("target", "feature"))
			_, err = tx.Commit(t.Context())
			assert.ErrorIs(t, err, ErrCommitFailed)
			assert.Equal(t, int32(1), cat.attempts.Load(),
				"ref fences must fail without reusing the stale ancestry check")
			assert.Equal(t, fastForwardTestRef(t, concurrent, "target").SnapshotID,
				fastForwardTestRef(t, cat.metadata, "target").SnapshotID)
		})
	}
}

func TestFastForwardBranchRejectsConcurrentDestinationCreation(t *testing.T) {
	base := fastForwardTestMetadata(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("new-feature", 100, BranchRef))
	concurrent, err := builder.Build()
	require.NoError(t, err)

	tbl, cat := newAssertRefTestTable(t, base, concurrent)
	tx, err := tbl.NewTransactionOnBranch("new-feature")
	require.NoError(t, err)
	require.NoError(t, tx.FastForwardBranch("new-feature", "feature"))
	_, err = tx.Commit(t.Context())
	assert.ErrorIs(t, err, ErrCommitFailed)
	assert.Equal(t, int64(100), fastForwardTestRef(t, cat.metadata, "new-feature").SnapshotID)
}

func TestFastForwardBranchDoesNotCommitWhenRefsAlreadyAgree(t *testing.T) {
	base := fastForwardTestMetadata(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("target", 300, BranchRef))
	sameHead, err := builder.Build()
	require.NoError(t, err)

	tbl, cat := newAssertRefTestTable(t, sameHead, sameHead)
	tx, err := tbl.NewTransactionOnBranch("target")
	require.NoError(t, err)
	require.NoError(t, tx.FastForwardBranch("target", "feature"))
	commit, err := tx.TableCommit()
	require.NoError(t, err)
	assert.Empty(t, commit.Updates)
	_, err = tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Zero(t, cat.attempts.Load())
}

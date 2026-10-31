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

func refManagementMetadata(t *testing.T) Metadata {
	t.Helper()

	base := newConflictTestMetadataWithChain(t, []int64{100, 200, 300})
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("staging", 100, BranchRef,
		WithMaxRefAgeMs(7000), WithMaxSnapshotAgeMs(9000), WithMinSnapshotsToKeep(2)))
	require.NoError(t, builder.SetSnapshotRef("audit", 300, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("release", 300, TagRef, WithMaxRefAgeMs(6000)))
	meta, err := builder.Build()
	require.NoError(t, err)

	return meta
}

func requireSnapshotRef(t *testing.T, meta Metadata, name string) SnapshotRef {
	t.Helper()
	for refName, ref := range meta.Refs() {
		if refName == name {
			return ref
		}
	}
	t.Fatalf("missing ref %q", name)

	return SnapshotRef{}
}

func assertMissingSnapshotRef(t *testing.T, meta Metadata, name string) {
	t.Helper()
	for refName := range meta.Refs() {
		assert.NotEqual(t, name, refName)
	}
}

func TestTransactionCreateSnapshotRefs(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{100, 200, 300})
	tbl, cat := newAssertRefTestTable(t, base, base)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CreateBranch("audit", 200,
		WithMinSnapshotsToKeep(3), WithMaxSnapshotAgeMs(9000), WithMaxRefAgeMs(12000)))
	require.NoError(t, tx.CreateTag("release", 100, WithMaxRefAgeMs(8000)))

	next, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(1), cat.attempts.Load())
	branch := requireSnapshotRef(t, next.Metadata(), "audit")
	assert.Equal(t, BranchRef, branch.Type)
	assert.Equal(t, int64(200), branch.SnapshotID)
	require.NotNil(t, branch.MinSnapshotsToKeep)
	assert.Equal(t, 3, *branch.MinSnapshotsToKeep)
	require.NotNil(t, branch.MaxSnapshotAgeMs)
	assert.Equal(t, int64(9000), *branch.MaxSnapshotAgeMs)
	require.NotNil(t, branch.MaxRefAgeMs)
	assert.Equal(t, int64(12000), *branch.MaxRefAgeMs)

	tag := requireSnapshotRef(t, next.Metadata(), "release")
	assert.Equal(t, TagRef, tag.Type)
	assert.Equal(t, int64(100), tag.SnapshotID)
	require.NotNil(t, tag.MaxRefAgeMs)
	assert.Equal(t, int64(8000), *tag.MaxRefAgeMs)
	assert.Nil(t, tag.MinSnapshotsToKeep)
	assert.Nil(t, tag.MaxSnapshotAgeMs)
	assert.Equal(t, int64(300), requireSnapshotRef(t, next.Metadata(), MainBranch).SnapshotID)
}

func TestTransactionCreateSnapshotRefsValidation(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{100, 200})
	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()

	tests := []struct {
		name string
		run  func() error
	}{
		{"blank name", func() error { return tx.CreateBranch(" ", 100) }},
		{"missing snapshot", func() error { return tx.CreateBranch("missing", 999) }},
		{"main is not a tag", func() error { return tx.CreateTag(MainBranch, 100) }},
		{"existing main", func() error { return tx.CreateBranch(MainBranch, 100) }},
		{"invalid ref age", func() error { return tx.CreateBranch("bad", 100, WithMaxRefAgeMs(0)) }},
		{"invalid snapshot age", func() error { return tx.CreateBranch("bad", 100, WithMaxSnapshotAgeMs(-1)) }},
		{"invalid minimum snapshots", func() error { return tx.CreateBranch("bad", 100, WithMinSnapshotsToKeep(0)) }},
		{"tag min snapshots", func() error { return tx.CreateTag("bad", 100, WithMinSnapshotsToKeep(2)) }},
		{"tag snapshot age", func() error { return tx.CreateTag("bad", 100, WithMaxSnapshotAgeMs(1000)) }},
		{"nil option", func() error { return tx.CreateBranch("bad", 100, nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := len(tx.meta.updates)
			require.ErrorIs(t, tt.run(), iceberg.ErrInvalidArgument)
			assert.Len(t, tx.meta.updates, before, "failed creation must not stage updates")
		})
	}

	require.NoError(t, tx.CreateTag("release", 100))
	require.ErrorIs(t, tx.CreateBranch("release", 200), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.CreateTag("release", 200), iceberg.ErrInvalidArgument)

	var nilTransaction *Transaction
	require.ErrorIs(t, nilTransaction.CreateBranch("branch", 100), ErrInvalidMetadata)
	require.ErrorIs(t, nilTransaction.CreateTag("tag", 100), ErrInvalidMetadata)
}

func TestTransactionFastForwardBranch(t *testing.T) {
	base := refManagementMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.FastForwardBranch("staging", "release"))

	next, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(1), cat.attempts.Load())
	staging := requireSnapshotRef(t, next.Metadata(), "staging")
	assert.Equal(t, int64(300), staging.SnapshotID)
	assert.Equal(t, BranchRef, staging.Type)
	require.NotNil(t, staging.MinSnapshotsToKeep)
	assert.Equal(t, 2, *staging.MinSnapshotsToKeep)
	require.NotNil(t, staging.MaxRefAgeMs)
	assert.Equal(t, int64(7000), *staging.MaxRefAgeMs)
	require.NotNil(t, staging.MaxSnapshotAgeMs)
	assert.Equal(t, int64(9000), *staging.MaxSnapshotAgeMs)
	assert.Equal(t, int64(300), requireSnapshotRef(t, next.Metadata(), "release").SnapshotID)

	tx = next.NewTransaction()
	require.NoError(t, tx.FastForwardBranch("new-branch", "release"))
	newer, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, BranchRef, requireSnapshotRef(t, newer.Metadata(), "new-branch").Type)
	assert.Equal(t, int64(300), requireSnapshotRef(t, newer.Metadata(), "new-branch").SnapshotID)
}

func TestTransactionFastForwardBranchValidation(t *testing.T) {
	base := refManagementMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()

	for _, tt := range []struct {
		target string
		source string
	}{
		{"audit", "staging"}, // cannot rewind to an ancestor
		{"release", "audit"}, // tags are not fast-forward targets
		{"audit", "unknown"},
		{"", "audit"},
		{"audit", ""},
	} {
		require.ErrorIs(t, tx.FastForwardBranch(tt.target, tt.source), iceberg.ErrInvalidArgument)
	}
	assert.Empty(t, tx.meta.updates)

	// Advancing a branch to itself (or to an equal source head) is a no-op.
	require.NoError(t, tx.FastForwardBranch("audit", "audit"))
	require.NoError(t, tx.FastForwardBranch("audit", "release"))
	_, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assert.Equal(t, int32(0), cat.attempts.Load())
}

func TestTransactionFastForwardMain(t *testing.T) {
	base := refManagementMetadata(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 200, BranchRef))
	base, err = builder.Build()
	require.NoError(t, err)

	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.FastForwardBranch(MainBranch, "audit"))

	next, err := tx.Commit(t.Context())
	require.NoError(t, err)
	require.NotNil(t, next.CurrentSnapshot())
	assert.Equal(t, int64(300), next.CurrentSnapshot().SnapshotID)
	assert.Equal(t, int64(300), requireSnapshotRef(t, next.Metadata(), "audit").SnapshotID)
}

func TestTransactionRenameAndRemoveRefs(t *testing.T) {
	base := refManagementMetadata(t)
	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.RenameBranch("staging", "experimental"))
	require.NoError(t, tx.ReplaceTag("release", 100))

	next, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assertMissingSnapshotRef(t, next.Metadata(), "staging")
	experimental := requireSnapshotRef(t, next.Metadata(), "experimental")
	assert.Equal(t, int64(100), experimental.SnapshotID)
	require.NotNil(t, experimental.MinSnapshotsToKeep)
	assert.Equal(t, 2, *experimental.MinSnapshotsToKeep)
	assert.Equal(t, int64(100), requireSnapshotRef(t, next.Metadata(), "release").SnapshotID)
	require.NotNil(t, requireSnapshotRef(t, next.Metadata(), "release").MaxRefAgeMs)

	tx = next.NewTransaction()
	require.NoError(t, tx.RemoveBranch("experimental"))
	require.NoError(t, tx.RemoveTag("release"))
	afterRemove, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assertMissingSnapshotRef(t, afterRemove.Metadata(), "experimental")
	assertMissingSnapshotRef(t, afterRemove.Metadata(), "release")
	assert.Equal(t, int64(300), requireSnapshotRef(t, afterRemove.Metadata(), MainBranch).SnapshotID)
	assert.Equal(t, int64(300), requireSnapshotRef(t, afterRemove.Metadata(), "audit").SnapshotID)
}

func TestTransactionRenameNewlyCreatedBranch(t *testing.T) {
	base := newConflictTestMetadataWithChain(t, []int64{100})
	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()
	require.NoError(t, tx.CreateBranch("new", 100))
	require.NoError(t, tx.RenameBranch("new", "renamed"))

	committed, err := tx.Commit(t.Context())
	require.NoError(t, err)
	assertMissingSnapshotRef(t, committed.Metadata(), "new")
	assert.Equal(t, int64(100), requireSnapshotRef(t, committed.Metadata(), "renamed").SnapshotID)
}

func TestTransactionSnapshotRefMutationValidation(t *testing.T) {
	base := refManagementMetadata(t)
	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()

	require.ErrorIs(t, tx.RenameBranch(MainBranch, "new"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RenameBranch("audit", MainBranch), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RenameBranch("release", "new"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RenameBranch("audit", "release"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RenameBranch("unknown", "new"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RemoveBranch(MainBranch), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RemoveTag(MainBranch), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RemoveBranch("release"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RemoveTag("audit"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.RemoveTag("unknown"), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.ReplaceTag("audit", 100), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.ReplaceTag("release", 999), iceberg.ErrInvalidArgument)
	assert.Empty(t, tx.meta.updates)
}

func TestTransactionSetRefRetention(t *testing.T) {
	base := refManagementMetadata(t)
	tbl, _ := newAssertRefTestTable(t, base, base)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.SetRefRetention("staging", WithMinSnapshotsToKeep(5)))
	require.NoError(t, tx.SetRefRetention("release", WithMaxRefAgeMs(15000)))
	require.NoError(t, tx.SetRefRetention("audit"))
	require.ErrorIs(t, tx.SetRefRetention("release", WithMaxSnapshotAgeMs(12000)), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.SetRefRetention("release", WithMinSnapshotsToKeep(3)), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.SetRefRetention(MainBranch, WithMaxRefAgeMs(12000)), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.SetRefRetention("unknown", WithMaxRefAgeMs(5000)), iceberg.ErrInvalidArgument)
	require.ErrorIs(t, tx.SetRefRetention("audit", WithMinSnapshotsToKeep(-1)), iceberg.ErrInvalidArgument)

	next, err := tx.Commit(t.Context())
	require.NoError(t, err)
	staging := requireSnapshotRef(t, next.Metadata(), "staging")
	require.NotNil(t, staging.MinSnapshotsToKeep)
	require.NotNil(t, staging.MaxRefAgeMs)
	require.NotNil(t, staging.MaxSnapshotAgeMs)
	assert.Equal(t, 5, *staging.MinSnapshotsToKeep)
	assert.Equal(t, int64(7000), *staging.MaxRefAgeMs)
	assert.Equal(t, int64(9000), *staging.MaxSnapshotAgeMs)
	release := requireSnapshotRef(t, next.Metadata(), "release")
	assert.Equal(t, int64(300), release.SnapshotID)
	require.NotNil(t, release.MaxRefAgeMs)
	assert.Equal(t, int64(15000), *release.MaxRefAgeMs)
}

func TestTransactionRefChangesFailOnConcurrentMutation(t *testing.T) {
	t.Run("creation cannot replace a peer ref", func(t *testing.T) {
		base := newConflictTestMetadataWithChain(t, []int64{100, 200})
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("new", 100, TagRef))
		peer, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.CreateBranch("new", 200))
		_, err = tx.Commit(t.Context())
		require.Error(t, err)
		assert.Equal(t, TagRef, requireSnapshotRef(t, cat.metadata, "new").Type)
	})

	t.Run("source advance fences fast-forward", func(t *testing.T) {
		base := refManagementMetadata(t)
		peer := graftSnapshotOnto(t, base, "audit", 400)
		tbl, cat := newAssertRefTestTable(t, base, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.FastForwardBranch("staging", "audit"))
		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.Equal(t, int64(100), requireSnapshotRef(t, cat.metadata, "staging").SnapshotID)
	})

	t.Run("target advance fences fast-forward", func(t *testing.T) {
		base := refManagementMetadata(t)
		// Staging is at snapshot 100 while the table's global last
		// sequence number is 3. A peer branch commit must allocate a
		// sequence number above the TABLE's last sequence number.
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		parentID := int64(100)
		require.NoError(t, builder.AddSnapshot(&Snapshot{
			SnapshotID:       400,
			ParentSnapshotID: &parentID,
			SequenceNumber:   4,
			TimestampMs:      base.LastUpdatedMillis() + 1,
			Summary:          &Summary{Operation: OpAppend},
		}))
		require.NoError(t, builder.SetSnapshotRef("staging", 400, BranchRef,
			WithMaxRefAgeMs(7000), WithMaxSnapshotAgeMs(9000), WithMinSnapshotsToKeep(2)))
		peer, err := builder.Build()
		require.NoError(t, err)
		tbl, cat := newAssertRefTestTable(t, base, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.FastForwardBranch("staging", "audit"))
		_, err := tx.Commit(t.Context())
		require.Error(t, err)
		assert.Equal(t, int64(400), requireSnapshotRef(t, cat.metadata, "staging").SnapshotID)
	})

	t.Run("tag move cannot overwrite a peer move", func(t *testing.T) {
		base := refManagementMetadata(t)
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("release", 200, TagRef, WithMaxRefAgeMs(6000)))
		peer, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.ReplaceTag("release", 100))
		_, err = tx.Commit(t.Context())
		require.Error(t, err)
		assert.Equal(t, int64(200), requireSnapshotRef(t, cat.metadata, "release").SnapshotID)
	})

	t.Run("rename cannot overwrite a peer destination", func(t *testing.T) {
		base := refManagementMetadata(t)
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("renamed", 300, TagRef))
		peer, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.RenameBranch("staging", "renamed"))
		_, err = tx.Commit(t.Context())
		require.Error(t, err)
		assert.Equal(t, TagRef, requireSnapshotRef(t, cat.metadata, "renamed").Type)
	})
}

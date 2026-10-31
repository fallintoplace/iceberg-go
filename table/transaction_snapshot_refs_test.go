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

func snapshotRefForTest(meta Metadata, name string) (SnapshotRef, bool) {
	for key, ref := range meta.Refs() {
		if key == name {
			return ref, true
		}
	}
	return SnapshotRef{}, false
}

func TestTransactionSnapshotRefLifecycle(t *testing.T) {
	head := int64(100)
	meta := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
	tbl, cat := newAssertRefTestTable(t, meta, meta)
	tx := tbl.NewTransaction()

	require.NoError(t, tx.CreateBranch("audit/2026", head,
		WithMaxRefAgeMs(604800000), WithMaxSnapshotAgeMs(86400000), WithMinSnapshotsToKeep(3)))
	require.NoError(t, tx.CreateTag("release/1", head, WithMaxRefAgeMs(2592000000)))
	require.NoError(t, tx.SetProperties(iceberg.Properties{"revision": "7"}))

	// Both new names must assert absence in the original catalog state.
	seen := map[string]bool{}
	for _, r := range tx.reqs {
		if assertion, ok := r.(*assertRefSnapshotID); ok {
			assert.Nil(t, assertion.SnapshotID)
			seen[assertion.Ref] = true
		}
	}
	assert.True(t, seen["audit/2026"])
	assert.True(t, seen["release/1"])

	_, err := tx.Commit(t.Context())
	require.NoError(t, err)

	branch, ok := snapshotRefForTest(cat.metadata, "audit/2026")
	require.True(t, ok)
	assert.Equal(t, BranchRef, branch.Type)
	assert.Equal(t, head, branch.SnapshotID)
	require.NotNil(t, branch.MaxRefAgeMs)
	require.NotNil(t, branch.MaxSnapshotAgeMs)
	require.NotNil(t, branch.MinSnapshotsToKeep)
	assert.Equal(t, int64(604800000), *branch.MaxRefAgeMs)
	assert.Equal(t, int64(86400000), *branch.MaxSnapshotAgeMs)
	assert.Equal(t, 3, *branch.MinSnapshotsToKeep)

	tag, ok := snapshotRefForTest(cat.metadata, "release/1")
	require.True(t, ok)
	assert.Equal(t, TagRef, tag.Type)
	assert.Equal(t, head, tag.SnapshotID)
	require.NotNil(t, tag.MaxRefAgeMs)
	assert.Equal(t, int64(2592000000), *tag.MaxRefAgeMs)
	assert.Nil(t, tag.MaxSnapshotAgeMs)
	assert.Nil(t, tag.MinSnapshotsToKeep)
	assert.Equal(t, "7", cat.metadata.Properties()["revision"])
	assert.Equal(t, head, cat.metadata.CurrentSnapshot().SnapshotID)
}

func TestTransactionSnapshotRefRetentionAndDeletion(t *testing.T) {
	head := int64(100)
	meta := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("audit", head, BranchRef,
		WithMaxRefAgeMs(1000), WithMaxSnapshotAgeMs(2000), WithMinSnapshotsToKeep(4)))
	require.NoError(t, builder.SetSnapshotRef("release", head, TagRef, WithMaxRefAgeMs(3000)))
	base, err := builder.Build()
	require.NoError(t, err)

	t.Run("update retains unspecified settings and can clear overrides", func(t *testing.T) {
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()

		require.NoError(t, tx.UpdateSnapshotRefRetention("audit",
			WithoutMaxRefAgeMs(), WithMaxSnapshotAgeMs(5000)))
		require.NoError(t, tx.UpdateSnapshotRefRetention("release", WithoutMaxRefAgeMs()))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)

		branch, ok := snapshotRefForTest(cat.metadata, "audit")
		require.True(t, ok)
		assert.Nil(t, branch.MaxRefAgeMs)
		require.NotNil(t, branch.MaxSnapshotAgeMs)
		require.NotNil(t, branch.MinSnapshotsToKeep)
		assert.Equal(t, int64(5000), *branch.MaxSnapshotAgeMs)
		assert.Equal(t, 4, *branch.MinSnapshotsToKeep)
		assert.Equal(t, head, branch.SnapshotID)

		tag, ok := snapshotRefForTest(cat.metadata, "release")
		require.True(t, ok)
		assert.Nil(t, tag.MaxRefAgeMs)
		assert.Equal(t, head, tag.SnapshotID)
	})

	t.Run("remove branch and tag together", func(t *testing.T) {
		tbl, cat := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.RemoveBranch("audit"))
		require.NoError(t, tx.RemoveTag("release"))
		_, err := tx.Commit(t.Context())
		require.NoError(t, err)
		_, ok := snapshotRefForTest(cat.metadata, "audit")
		assert.False(t, ok)
		_, ok = snapshotRefForTest(cat.metadata, "release")
		assert.False(t, ok)
		_, ok = snapshotRefForTest(cat.metadata, MainBranch)
		assert.True(t, ok)
	})

	t.Run("unchanged policy is a no-op", func(t *testing.T) {
		tbl, _ := newAssertRefTestTable(t, base, base)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.UpdateSnapshotRefRetention("audit"))
		require.NoError(t, tx.UpdateSnapshotRefRetention("audit", WithMaxRefAgeMs(1000)))
		assert.Empty(t, tx.meta.updates)
		assert.Empty(t, tx.pinnedRefs)
	})
}

func TestTransactionSnapshotRefInvalidChangesAreAtomic(t *testing.T) {
	head := int64(100)
	meta := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)
	tbl, _ := newAssertRefTestTable(t, meta, meta)
	tx := tbl.NewTransaction()

	for _, name := range []string{"", " ", ".", "..", "leading ", "a\nb"} {
		assert.ErrorIs(t, tx.CreateBranch(name, head), iceberg.ErrInvalidArgument, name)
	}
	assert.ErrorIs(t, tx.CreateBranch(MainBranch, head), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.CreateTag("missing-snapshot", 999), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.CreateTag("invalid-tag", head, WithMinSnapshotsToKeep(2)), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.CreateTag("invalid-tag", head, WithMaxSnapshotAgeMs(200)), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.CreateBranch("invalid-branch", head, WithMinSnapshotsToKeep(0)), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.CreateTag("nil-option", head, nil), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.RemoveBranch(MainBranch), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.RemoveBranch("does-not-exist"), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.UpdateSnapshotRefRetention("does-not-exist", WithoutMaxRefAgeMs()), iceberg.ErrInvalidArgument)
	assert.Empty(t, tx.meta.updates)
	assert.Empty(t, tx.reqs)
	assert.Empty(t, tx.pinnedRefs)

	require.NoError(t, tx.CreateTag("release", head))
	assert.ErrorIs(t, tx.CreateTag("release", head), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.CreateBranch("release", head), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.RemoveBranch("release"), iceberg.ErrInvalidArgument)
	assert.ErrorIs(t, tx.UpdateSnapshotRefRetention("release", WithMinSnapshotsToKeep(2)), iceberg.ErrInvalidArgument)
	assert.Len(t, tx.meta.updates, 1, "failed operations must not stage updates")
	assert.Len(t, tx.reqs, 1)
}

func TestTransactionSnapshotRefPinnedAgainstConcurrentChanges(t *testing.T) {
	head := int64(100)
	base := newConflictTestMetadataWithProps(t, &head, assertRefRetryProps)

	t.Run("concurrent creation cannot overwrite an existing ref", func(t *testing.T) {
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("audit", head, BranchRef))
		peer, err := builder.Build()
		require.NoError(t, err)

		tbl, cat := newAssertRefTestTable(t, base, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.CreateBranch("audit", head))
		_, err = tx.Commit(t.Context())
		assert.ErrorIs(t, err, ErrCommitFailed)
		ref, found := snapshotRefForTest(cat.metadata, "audit")
		assert.True(t, found)
		assert.Equal(t, head, ref.SnapshotID)
		assert.Equal(t, int32(1), cat.attempts.Load())
	})

	t.Run("concurrent branch append prevents deletion", func(t *testing.T) {
		builder, err := MetadataBuilderFromBase(base, "")
		require.NoError(t, err)
		require.NoError(t, builder.SetSnapshotRef("audit", head, BranchRef))
		observed, err := builder.Build()
		require.NoError(t, err)
		peer := graftSnapshotOnto(t, observed, "audit", 200)

		tbl, cat := newAssertRefTestTable(t, observed, peer)
		tx := tbl.NewTransaction()
		require.NoError(t, tx.RemoveBranch("audit"))
		_, err = tx.Commit(t.Context())
		assert.ErrorIs(t, err, ErrCommitFailed)
		ref, found := snapshotRefForTest(cat.metadata, "audit")
		assert.True(t, found)
		assert.Equal(t, int64(200), ref.SnapshotID)
		assert.Equal(t, int32(1), cat.attempts.Load())
	})
}

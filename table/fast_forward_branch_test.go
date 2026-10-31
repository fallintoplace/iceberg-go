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
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func fastForwardTestMetadata(t *testing.T) Metadata {
	t.Helper()

	meta := newConflictTestMetadataWithChain(t, []int64{1, 2, 3})
	builder, err := MetadataBuilderFromBase(meta, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("feature", 1, BranchRef,
		WithMaxRefAgeMs(1_000), WithMaxSnapshotAgeMs(2_000), WithMinSnapshotsToKeep(2)))
	require.NoError(t, builder.SetSnapshotRef("audit", 3, BranchRef))
	require.NoError(t, builder.SetSnapshotRef("release", 3, TagRef, WithMaxRefAgeMs(5_000)))

	result, err := builder.Build()
	require.NoError(t, err)

	return result
}

func fastForwardRef(t *testing.T, meta Metadata, name string) SnapshotRef {
	t.Helper()
	for refName, ref := range meta.Refs() {
		if refName == name {
			return ref
		}
	}
	t.Fatalf("ref %q is missing", name)

	return SnapshotRef{}
}

func TestFastForwardBranchPreservesRetentionAndSource(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)
	original := fastForwardRef(t, base, "feature")

	updated, err := tbl.FastForwardBranch(t.Context(), "feature", "audit")
	require.NoError(t, err)
	require.NotNil(t, updated)
	require.Equal(t, int32(1), cat.attempts.Load())
	require.Equal(t, int64(3), fastForwardRef(t, updated.Metadata(), "feature").SnapshotID)
	require.Equal(t, int64(3), fastForwardRef(t, updated.Metadata(), "audit").SnapshotID)
	require.Equal(t, int64(1), fastForwardRef(t, base, "feature").SnapshotID,
		"the original table value must not mutate")

	advanced := fastForwardRef(t, updated.Metadata(), "feature")
	require.Equal(t, original.MaxRefAgeMs, advanced.MaxRefAgeMs)
	require.Equal(t, original.MaxSnapshotAgeMs, advanced.MaxSnapshotAgeMs)
	require.Equal(t, original.MinSnapshotsToKeep, advanced.MinSnapshotsToKeep)
	require.Len(t, updated.Metadata().Snapshots(), len(base.Snapshots()), "fast-forward must not create a snapshot")
}

func TestFastForwardBranchAcceptsTagSource(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)

	updated, err := tbl.FastForwardBranch(t.Context(), "feature", "release")
	require.NoError(t, err)
	require.Equal(t, int32(1), cat.attempts.Load())
	require.Equal(t, int64(3), fastForwardRef(t, updated.Metadata(), "feature").SnapshotID)
	require.Equal(t, TagRef, fastForwardRef(t, updated.Metadata(), "release").Type)
	require.Equal(t, int64(3), fastForwardRef(t, updated.Metadata(), "release").SnapshotID)
}

func TestFastForwardBranchCreatesMissingDestination(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)

	updated, err := tbl.FastForwardBranch(t.Context(), "new-audit", "release")
	require.NoError(t, err)
	require.Equal(t, int32(1), cat.attempts.Load())
	created := fastForwardRef(t, updated.Metadata(), "new-audit")
	require.Equal(t, int64(3), created.SnapshotID)
	require.Equal(t, BranchRef, created.Type)
	require.Nil(t, created.MaxRefAgeMs)
	require.Nil(t, created.MaxSnapshotAgeMs)
	require.Nil(t, created.MinSnapshotsToKeep)
}

func TestFastForwardBranchAdvancesMainAndHistory(t *testing.T) {
	base := fastForwardTestMetadata(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef(MainBranch, 1, BranchRef))
	base, err = builder.Build()
	require.NoError(t, err)
	tbl, cat := newAssertRefTestTable(t, base, base)

	updated, err := tbl.FastForwardBranch(t.Context(), MainBranch, "audit")
	require.NoError(t, err)
	require.Equal(t, int32(1), cat.attempts.Load())
	require.Equal(t, int64(3), updated.CurrentSnapshot().SnapshotID)
	require.Equal(t, int64(3), fastForwardRef(t, updated.Metadata(), MainBranch).SnapshotID)

	var last SnapshotLogEntry
	for entry := range updated.Metadata().SnapshotLogs() {
		last = entry
	}
	require.Equal(t, int64(3), last.SnapshotID)
}

func TestFastForwardBranchNoOp(t *testing.T) {
	base := fastForwardTestMetadata(t)
	for _, tc := range []struct {
		name, target, source string
	}{
		{"same branch", "feature", "feature"},
		{"different refs same snapshot", "main", "audit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, cat := newAssertRefTestTable(t, base, base)
			updated, err := tbl.FastForwardBranch(t.Context(), tc.target, tc.source)
			require.NoError(t, err)
			require.Equal(t, int32(0), cat.attempts.Load())
			require.True(t, updated.Equals(*tbl))
		})
	}
}

func TestFastForwardBranchRejectsNonFastForward(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)

	_, err := tbl.FastForwardBranch(t.Context(), "audit", "feature")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "not an ancestor")
	require.Equal(t, int32(0), cat.attempts.Load())
}

func TestFastForwardBranchRejectsDivergentBranches(t *testing.T) {
	base := fastForwardTestMetadata(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	parent := int64(1)
	require.NoError(t, builder.AddSnapshot(&Snapshot{
		SnapshotID: 4, ParentSnapshotID: &parent, SequenceNumber: 4,
		TimestampMs: base.LastUpdatedMillis() + 1, Summary: &Summary{Operation: OpAppend},
	}))
	require.NoError(t, builder.SetSnapshotRef("fork", 4, BranchRef))
	base, err = builder.Build()
	require.NoError(t, err)
	tbl, cat := newAssertRefTestTable(t, base, base)

	_, err = tbl.FastForwardBranch(t.Context(), "fork", "audit")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "not an ancestor")
	require.Equal(t, int32(0), cat.attempts.Load())
}

func TestFastForwardBranchRejectsTagTargetAndMissingSource(t *testing.T) {
	base := fastForwardTestMetadata(t)
	for _, tc := range []struct {
		name, target, source string
		wantSubstring        string
	}{
		{"tag destination", "release", "audit", "not a branch"},
		{"same tag", "release", "release", "not a branch"},
		{"missing source", "feature", "missing", "does not exist"},
		{"blank target", "", "audit", "names must"},
		{"blank source", "feature", "", "names must"},
		{"invalid whitespace", " feature", "audit", "names must"},
		{"invalid control", "feature", "aud\nit", "names must"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tbl, cat := newAssertRefTestTable(t, base, base)
			_, err := tbl.FastForwardBranch(t.Context(), tc.target, tc.source)
			require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
			require.ErrorContains(t, err, tc.wantSubstring)
			require.Equal(t, int32(0), cat.attempts.Load())
		})
	}
}

func TestFastForwardBranchFencesBothRefs(t *testing.T) {
	base := fastForwardTestMetadata(t)
	for _, tc := range []struct {
		name, movedRef string
		to             int64
	}{
		{"target advanced", "feature", 2},
		{"source rewound", "audit", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, err := MetadataBuilderFromBase(base, "")
			require.NoError(t, err)
			ref := fastForwardRef(t, base, tc.movedRef)
			var opts []setSnapshotRefOption
			if ref.MaxRefAgeMs != nil {
				opts = append(opts, WithMaxRefAgeMs(*ref.MaxRefAgeMs))
			}
			if ref.MaxSnapshotAgeMs != nil {
				opts = append(opts, WithMaxSnapshotAgeMs(*ref.MaxSnapshotAgeMs))
			}
			if ref.MinSnapshotsToKeep != nil {
				opts = append(opts, WithMinSnapshotsToKeep(*ref.MinSnapshotsToKeep))
			}
			require.NoError(t, builder.SetSnapshotRef(tc.movedRef, tc.to, BranchRef, opts...))
			catalogMeta, err := builder.Build()
			require.NoError(t, err)
			tbl, cat := newAssertRefTestTable(t, base, catalogMeta)

			_, err = tbl.FastForwardBranch(t.Context(), "feature", "audit")
			require.ErrorIs(t, err, ErrCommitFailed)
			require.Equal(t, tc.to, fastForwardRef(t, cat.metadata, tc.movedRef).SnapshotID)
			require.Equal(t, int32(1), cat.attempts.Load())
		})
	}
}

func TestFastForwardBranchFencesAbsentDestination(t *testing.T) {
	base := fastForwardTestMetadata(t)
	builder, err := MetadataBuilderFromBase(base, "")
	require.NoError(t, err)
	require.NoError(t, builder.SetSnapshotRef("new-branch", 2, BranchRef))
	concurrent, err := builder.Build()
	require.NoError(t, err)
	tbl, cat := newAssertRefTestTable(t, base, concurrent)

	_, err = tbl.FastForwardBranch(t.Context(), "new-branch", "audit")
	require.ErrorIs(t, err, ErrCommitFailed)
	require.Equal(t, int64(2), fastForwardRef(t, cat.metadata, "new-branch").SnapshotID)
	require.Equal(t, int32(1), cat.attempts.Load())
}

func TestFastForwardBranchCancellation(t *testing.T) {
	base := fastForwardTestMetadata(t)
	tbl, cat := newAssertRefTestTable(t, base, base)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := tbl.FastForwardBranch(ctx, "feature", "audit")
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(0), cat.attempts.Load())

	_, err = tbl.FastForwardBranch(nil, "feature", "audit")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.Equal(t, int32(0), cat.attempts.Load())
}

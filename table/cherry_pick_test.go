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

package table_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCherryPickSnapshotFromDivergentBranch(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	base := tbl.CurrentSnapshot().SnapshotID
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)
	require.Equal(t, table.OpAppend, source.Summary.Operation)

	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	targetBefore := tbl.CurrentSnapshot().SnapshotID
	require.NotEqual(t, base, targetBefore)

	tx := tbl.NewTransaction()
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, iceberg.Properties{"cherry-picked": "yes"}))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	require.NotEqual(t, targetBefore, result.CurrentSnapshot().SnapshotID)
	require.NotEqual(t, source.SnapshotID, result.CurrentSnapshot().SnapshotID)
	require.Equal(t, table.OpAppend, result.CurrentSnapshot().Summary.Operation)
	assert.Equal(t, "yes", result.CurrentSnapshot().Summary.Properties["cherry-picked"])
	assert.ElementsMatch(t, []int64{1, 2, 3}, idsOnRef(t, result, table.MainBranch))
	assert.ElementsMatch(t, []int64{1, 2}, idsOnRef(t, result, branchPlanningRef))

	// The source snapshot's inherited file must not be cherry-picked twice.
	// Replaying the same source after a successful commit also fails rather
	// than creating a snapshot with two live references to the same file.
	duplicate := result.NewTransaction()
	err = duplicate.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorContains(t, err, "already referenced")
}

func TestCherryPickSnapshotRejectsAncestor(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	id := tbl.CurrentSnapshot().SnapshotID

	tx := tbl.NewTransaction()
	err := tx.CherryPickSnapshot(t.Context(), id, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
	require.ErrorContains(t, err, "already an ancestor")
}

func TestCherryPickSnapshotAllowsEmptyAppend(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 11)
	oldBranch := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, oldBranch)

	empty := tbl.NewTransaction()
	require.NoError(t, empty.AddDataFilesByPath(t.Context(), nil, nil, false))
	tbl, err := empty.Commit(t.Context())
	require.NoError(t, err)
	source := tbl.CurrentSnapshot()
	require.NotNil(t, source)
	require.Equal(t, table.OpAppend, source.Summary.Operation)

	tx, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))
	result, err := tx.Commit(t.Context())
	require.NoError(t, err)

	newBranch := result.SnapshotByName(branchPlanningRef)
	require.NotNil(t, newBranch)
	assert.NotEqual(t, oldBranch.SnapshotID, newBranch.SnapshotID)
	assert.Equal(t, oldBranch.SnapshotID, *newBranch.ParentSnapshotID)
	assert.ElementsMatch(t, []int64{11}, idsOnRef(t, result, branchPlanningRef))
}

func TestCherryPickSnapshotRejectsConcurrentTargetMove(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	stale := tbl.NewTransaction()
	require.NoError(t, stale.CherryPickSnapshot(t.Context(), source.SnapshotID, nil))

	// A concurrent commit changes main after the cherry-pick validated its
	// duplicate-file set. The pinned head forbids an unsafe automatic rebase.
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)
	_, err := stale.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrCommitFailed)
	assert.ElementsMatch(t, []int64{1, 3}, idsOnRef(t, tbl, table.MainBranch))
}

func TestCherryPickSnapshotRejectsV3RowLineageReuse(t *testing.T) {
	tbl := newBranchPlanningTable(t, iceberg.Properties{table.PropertyFormatVersion: "3"})
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	source := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, source)

	tx := tbl.NewTransaction()
	err := tx.CherryPickSnapshot(t.Context(), source.SnapshotID, nil)
	require.ErrorIs(t, err, iceberg.ErrNotImplemented)
	assert.Contains(t, err.Error(), "row ID")
}

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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestFastForwardBranchPromotesWithoutNewSnapshot(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	sourceHead := tbl.SnapshotByName(branchPlanningRef).SnapshotID
	before := len(tbl.Metadata().Snapshots())

	txn := tbl.NewTransaction()
	require.NoError(t, txn.FastForwardBranch(branchPlanningRef))
	promoted, err := txn.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, sourceHead, promoted.CurrentSnapshot().SnapshotID)
	require.Len(t, promoted.Metadata().Snapshots(), before)
	require.Equal(t, []int64{1, 2}, idsOnRef(t, promoted, table.MainBranch))
	require.Equal(t, []int64{1, 2}, idsOnRef(t, promoted, branchPlanningRef))
}

func TestFastForwardBranchRejectsDivergedHistory(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
	head := tbl.CurrentSnapshot().SnapshotID

	txn := tbl.NewTransaction()
	err := txn.FastForwardBranch(branchPlanningRef)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	staged, err := txn.StagedTable()
	require.NoError(t, err)
	require.Equal(t, head, staged.CurrentSnapshot().SnapshotID)
}

func TestFastForwardBranchRejectsUnknownRef(t *testing.T) {
	tbl := appendRowsOnRef(t, newBranchPlanningTable(t, nil), table.MainBranch, 1)
	txn := tbl.NewTransaction()
	err := txn.FastForwardBranch("missing")
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
}

func TestCherryPickAppendBranchReplaysMultipleDivergedSnapshots(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3)
	sourceHead := tbl.SnapshotByName(branchPlanningRef).SnapshotID
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 4)
	before := len(tbl.Metadata().Snapshots())

	txn := tbl.NewTransaction()
	require.NoError(t, txn.CherryPickAppendBranch(t.Context(), branchPlanningRef, iceberg.Properties{"campaign": "release"}))
	promoted, err := txn.Commit(t.Context())
	require.NoError(t, err)

	require.Len(t, promoted.Metadata().Snapshots(), before+1)
	require.Equal(t, []int64{1, 2, 3, 4}, idsOnRef(t, promoted, table.MainBranch))
	require.Equal(t, []int64{1, 2, 3}, idsOnRef(t, promoted, branchPlanningRef))
	require.Equal(t, table.OpAppend, promoted.CurrentSnapshot().Summary.Operation)
	require.Equal(t, strconv.FormatInt(sourceHead, 10), promoted.CurrentSnapshot().Summary.Properties["source-snapshot-id"])
	require.Equal(t, "release", promoted.CurrentSnapshot().Summary.Properties["campaign"])
}

func TestCherryPickAppendBranchPreventsDuplicatePublication(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))

	txn := tbl.NewTransaction()
	require.NoError(t, txn.CherryPickAppendBranch(t.Context(), branchPlanningRef, nil))
	promoted, err := txn.Commit(t.Context())
	require.NoError(t, err)

	repeat := promoted.NewTransaction()
	err = repeat.CherryPickAppendBranch(t.Context(), branchPlanningRef, nil)
	require.Error(t, err, "the same data file cannot be live twice")
	require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, idsOnRef(t, promoted, table.MainBranch))
}

func TestCherryPickAppendBranchAlreadyIncludedIsNoOp(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	txn := tbl.NewTransaction()
	require.NoError(t, txn.FastForwardBranch(branchPlanningRef))
	promoted, err := txn.Commit(t.Context())
	require.NoError(t, err)

	noOp := promoted.NewTransaction()
	require.NoError(t, noOp.CherryPickAppendBranch(t.Context(), branchPlanningRef, nil))
	same, err := noOp.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, promoted.CurrentSnapshot().SnapshotID, same.CurrentSnapshot().SnapshotID)
}

func TestCherryPickAppendBranchRejectsDeleteHistory(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 1)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 2)
	tbl = appendRowsOnRef(t, tbl, table.MainBranch, 3)

	txn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.Delete(t.Context(), iceberg.AlwaysTrue{}, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	promote := tbl.NewTransaction()
	err = promote.CherryPickAppendBranch(t.Context(), branchPlanningRef, nil)
	require.ErrorIs(t, err, table.ErrInvalidOperation)
}

func TestCherryPickAppendBranchPinsBothRefs(t *testing.T) {
	tbl := seedDivergentBranch(t, newBranchPlanningTable(t, nil))
	txn := tbl.NewTransaction()
	require.NoError(t, txn.CherryPickAppendBranch(t.Context(), branchPlanningRef, nil))

	// A concurrent append to main must make publication fail rather than
	// letting the producer rebase and accidentally duplicate files.
	advanced := appendRowsOnRef(t, tbl, table.MainBranch, 7)
	_, err := txn.Commit(t.Context())
	require.Error(t, err)
	require.Equal(t, []int64{1, 2, 5, 6, 7}, idsOnRef(t, advanced, table.MainBranch))
}

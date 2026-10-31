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

package table_test

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPathRewriteFixture(t *testing.T) (*table.Table, string, string) {
	t.Helper()

	tbl, _ := newConcurrentRewriteTestTable(t)
	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)

	oldPath := tbl.Location() + "/data/original.parquet"
	replacementPath := tbl.Location() + "/data/replacement.parquet"
	rows := `[{"id":1,"data":"first"},{"id":2,"data":"second"}]`
	writeParquetFile(t, oldPath, sc, rows)
	writeParquetFile(t, replacementPath, sc, rows)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.AddDataFilesByPath(t.Context(), []string{oldPath}, nil, false))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	return tbl, oldPath, replacementPath
}

func TestRewriteDataFilesByPathProducesReplaceSnapshot(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	before := tbl.CurrentSnapshot()
	require.NotNil(t, before)

	txn := tbl.NewTransaction()
	require.NoError(t, txn.RewriteDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath},
		iceberg.Properties{"reason": "compaction"}))

	tbl, err := txn.Commit(t.Context())
	require.NoError(t, err)

	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, table.OpReplace, snapshot.Summary.Operation)
	assert.Equal(t, "compaction", snapshot.Summary.Properties["reason"])
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
	dataPaths, _ := liveFilesOnRef(t, tbl, table.MainBranch)
	assert.Equal(t, []string{replacementPath}, dataPaths)
	assert.NotEqual(t, before.SnapshotID, snapshot.SnapshotID)
}

func TestNewRewriteFromPathsAllowsDataSequenceNumber(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	before := tbl.CurrentSnapshot()
	require.NotNil(t, before)

	txn := tbl.NewTransaction()
	rewrite, err := txn.NewRewriteFromPaths(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil)
	require.NoError(t, err)

	require.NoError(t, rewrite.DataSequenceNumber(before.SequenceNumber).Commit(t.Context()))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	assert.Equal(t, table.OpReplace, tbl.CurrentSnapshot().Summary.Operation)
	assert.Equal(t, []int64{1, 2}, idsOnRef(t, tbl, table.MainBranch))
}

func TestNewRewriteFromPathsValidatesPaths(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)

	cases := []struct {
		name    string
		removed []string
		added   []string
	}{
		{"no removed files", nil, []string{replacementPath}},
		{"no added files", []string{oldPath}, nil},
		{"blank removed path", []string{""}, []string{replacementPath}},
		{"blank added path", []string{oldPath}, []string{""}},
		{"duplicate removed path", []string{oldPath, oldPath}, []string{replacementPath}},
		{"duplicate added path", []string{oldPath}, []string{replacementPath, replacementPath}},
		{"overlapping path sets", []string{oldPath}, []string{oldPath}},
		{"unknown removed path", []string{tbl.Location() + "/unknown.parquet"}, []string{replacementPath}},
		{"existing added path", []string{oldPath}, []string{oldPath}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txn := tbl.NewTransaction()
			_, err := txn.NewRewriteFromPaths(t.Context(), tc.removed, tc.added, nil)
			require.ErrorIs(t, err, table.ErrInvalidOperation)
		})
	}

	t.Run("missing replacement parquet", func(t *testing.T) {
		txn := tbl.NewTransaction()
		_, err := txn.NewRewriteFromPaths(t.Context(), []string{oldPath},
			[]string{tbl.Location() + "/missing.parquet"}, nil)
		require.Error(t, err)
	})

	t.Run("no snapshot", func(t *testing.T) {
		empty, _ := newConcurrentRewriteTestTable(t)
		txn := empty.NewTransaction()
		_, err := txn.NewRewriteFromPaths(t.Context(), []string{"old"}, []string{"new"}, nil)
		require.ErrorIs(t, err, table.ErrInvalidOperation)
	})
}

func TestRewriteDataFilesByPathRejectsConcurrentEqualityDelete(t *testing.T) {
	tbl, oldPath, replacementPath := newPathRewriteFixture(t)
	leader := tbl.NewTransaction()
	require.NoError(t, leader.RewriteDataFilesByPath(t.Context(),
		[]string{oldPath}, []string{replacementPath}, nil))

	peer := tbl.NewTransaction()
	deletes := newEqDeleteFile(t, tbl.Location()+"/data/concurrent-eq-delete.parquet")
	require.NoError(t, peer.NewRowDelta(nil).AddDeletes(deletes).Commit(t.Context()))
	_, err := peer.Commit(t.Context())
	require.NoError(t, err)

	_, err = leader.Commit(t.Context())
	require.ErrorIs(t, err, table.ErrConflictingDeleteFiles)
}

func TestRewriteDataFilesByPathTargetsBranch(t *testing.T) {
	tbl := newBranchPlanningTable(t, nil)
	tbl = appendRowsOnRef(t, tbl, branchPlanningRef, 3, 4)
	require.Nil(t, tbl.CurrentSnapshot())

	oldPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	require.Len(t, oldPaths, 1)

	sc, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	require.NoError(t, err)
	replacement := tbl.Location() + "/data/branch-replacement.parquet"
	writeParquetFile(t, replacement, sc, `[{"id":3},{"id":4}]`)

	txn, err := tbl.NewTransactionOnBranch(branchPlanningRef)
	require.NoError(t, err)
	require.NoError(t, txn.RewriteDataFilesByPath(t.Context(), oldPaths,
		[]string{replacement}, nil))
	tbl, err = txn.Commit(t.Context())
	require.NoError(t, err)

	require.Nil(t, tbl.CurrentSnapshot(), "rewriting a branch must not create a main snapshot")
	assert.Equal(t, []int64{3, 4}, idsOnRef(t, tbl, branchPlanningRef))
	dataPaths, _ := liveFilesOnRef(t, tbl, branchPlanningRef)
	assert.Equal(t, []string{replacement}, dataPaths)
	branchHead := tbl.SnapshotByName(branchPlanningRef)
	require.NotNil(t, branchHead)
	assert.Equal(t, table.OpReplace, branchHead.Summary.Operation)
}

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
	"fmt"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rollbackToTimeTxn builds a committed v2 snapshot lineage and returns a new
// transaction. Offsets are measured relative to an epoch in the recent past,
// keeping rollback timestamps and the snapshot log in chronological order.
func rollbackToTimeTxn(t *testing.T, offsets []int64) (*Transaction, int64) {
	t.Helper()

	builder := builderWithoutChanges(2)
	epoch := time.Now().UnixMilli() - 15_000
	for i, offset := range offsets {
		id := int64(i + 1)
		snap := &Snapshot{
			SnapshotID:     id,
			SequenceNumber: id,
			TimestampMs:    epoch + offset,
			ManifestList:   fmt.Sprintf("s3://bucket/metadata/snap-%d.avro", id),
			Summary:        &Summary{Operation: OpAppend},
		}
		if i > 0 {
			parentID := id - 1
			snap.ParentSnapshotID = &parentID
		}
		require.NoError(t, builder.AddSnapshot(snap))
		require.NoError(t, builder.SetSnapshotRef(MainBranch, id, BranchRef))
	}

	metadata, err := builder.Build()
	require.NoError(t, err)
	staged, err := MetadataBuilderFromBase(metadata, "")
	require.NoError(t, err)

	return &Transaction{meta: staged}, epoch
}

func TestRollbackToTimeSelectsLatestAncestorBeforeTimestamp(t *testing.T) {
	tests := []struct {
		name           string
		timestamps     []int64
		cutoff         int64
		wantSnapshotID int64
		wantError      bool
	}{
		{name: "before first", timestamps: []int64{1_000, 2_000, 3_000}, cutoff: 1_000, wantError: true},
		{name: "strictly before second", timestamps: []int64{1_000, 2_000, 3_000}, cutoff: 2_000, wantSnapshotID: 1},
		{name: "between second and third", timestamps: []int64{1_000, 2_000, 3_000}, cutoff: 2_500, wantSnapshotID: 2},
		{name: "strictly before third", timestamps: []int64{1_000, 2_000, 3_000}, cutoff: 3_000, wantSnapshotID: 2},
		{name: "current snapshot is eligible", timestamps: []int64{1_000, 2_000, 3_000}, cutoff: 3_001, wantSnapshotID: 3},
		{name: "max timestamp not newest ancestor", timestamps: []int64{1_000, 3_000, 2_500}, cutoff: 3_500, wantSnapshotID: 2},
		{name: "equal timestamps keep newest ancestor", timestamps: []int64{1_000, 2_000, 2_000}, cutoff: 2_500, wantSnapshotID: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn, epoch := rollbackToTimeTxn(t, tt.timestamps)
			updatesBefore := len(txn.meta.updates)

			err := txn.RollbackToTime(epoch + tt.cutoff)
			if tt.wantError {
				require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
				assert.Equal(t, int64(3), txn.meta.currentSnapshot().SnapshotID)
				assert.Len(t, txn.meta.updates, updatesBefore)
				assert.Empty(t, txn.reqs)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, txn.meta.currentSnapshot())
			assert.Equal(t, tt.wantSnapshotID, txn.meta.currentSnapshot().SnapshotID)
			assert.Len(t, txn.reqs, 1, "rollback must retain the main-branch conflict requirement")
			updated, err := txn.meta.Build()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSnapshotID, updated.CurrentSnapshot().SnapshotID)
		})
	}
}

func TestRollbackToTimeIgnoresSnapshotsOutsideCurrentAncestry(t *testing.T) {
	txn, epoch := rollbackToTimeTxn(t, []int64{1_000, 2_000, 3_000})

	rootID := int64(1)
	require.NoError(t, txn.meta.AddSnapshot(&Snapshot{
		SnapshotID:       4,
		ParentSnapshotID: &rootID,
		SequenceNumber:   4,
		TimestampMs:      epoch + 3_400,
		ManifestList:     "s3://bucket/metadata/audit.avro",
		Summary:          &Summary{Operation: OpAppend},
	}))
	require.NoError(t, txn.meta.SetSnapshotRef("audit", 4, BranchRef))

	require.NoError(t, txn.RollbackToTime(epoch+3_500))
	assert.Equal(t, int64(3), txn.meta.currentSnapshot().SnapshotID)
}

func TestRollbackToTimePreservesStagedPropertyUpdates(t *testing.T) {
	txn, epoch := rollbackToTimeTxn(t, []int64{1_000, 2_000, 3_000})
	require.NoError(t, txn.SetProperties(iceberg.Properties{"custom.rollback-test": "enabled"}))

	require.NoError(t, txn.RollbackToTime(epoch+2_500))
	assert.Equal(t, int64(2), txn.meta.currentSnapshot().SnapshotID)
	assert.Equal(t, "enabled", txn.meta.props["custom.rollback-test"])
}

func TestRollbackToTimeRequiresCurrentSnapshot(t *testing.T) {
	builder := builderWithoutChanges(2)
	txn := &Transaction{meta: &builder}

	require.ErrorContains(t, txn.RollbackToTime(time.Now().UnixMilli()), "no current snapshot")
	assert.Empty(t, txn.reqs)
}

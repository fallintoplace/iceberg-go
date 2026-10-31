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

package main

import (
    "context"
    "testing"

    "github.com/apache/iceberg-go"
    "github.com/apache/iceberg-go/table"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

type branchForwardCapture struct {
    textOutput
    message string
    lastErr error
}

func (o *branchForwardCapture) Text(s string) { o.message = s }
func (o *branchForwardCapture) Error(err error) { o.lastErr = err }

func branchFastForwardTestTable(t *testing.T) *table.Table {
    t.Helper()
    schema := iceberg.NewSchema(0, iceberg.NestedField{
        ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
    })
    meta, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec,
        table.UnsortedSortOrder, t.TempDir(),
        iceberg.Properties{table.PropertyFormatVersion: "2"})
    require.NoError(t, err)
    builder, err := table.MetadataBuilderFromBase(meta, "")
    require.NoError(t, err)
    prev := int64(100)
    snapshots := []table.Snapshot{
        {SnapshotID: prev, SequenceNumber: 1, TimestampMs: meta.LastUpdatedMillis() + 1,
            Summary: &table.Summary{Operation: table.OpAppend}},
        {SnapshotID: 200, ParentSnapshotID: &prev, SequenceNumber: 2,
            TimestampMs: meta.LastUpdatedMillis() + 2,
            Summary: &table.Summary{Operation: table.OpAppend}},
    }
    for i := range snapshots {
        require.NoError(t, builder.AddSnapshot(&snapshots[i]))
    }
    require.NoError(t, builder.SetSnapshotRef(table.MainBranch, 100, table.BranchRef))
    require.NoError(t, builder.SetSnapshotRef("feature", 200, table.BranchRef))
    require.NoError(t, builder.SetSnapshotRef("staging", 100, table.BranchRef))
    result, err := builder.Build()
    require.NoError(t, err)
    return table.New(table.Identifier{"db", "events"}, result, "metadata.json", nil, nil)
}

func TestRunBranchFastForward(t *testing.T) {
    tbl := branchFastForwardTestTable(t)
    cat := &branchTagCommitCatalog{tbl: tbl}
    output := &branchForwardCapture{}
    exit := captureBranchTagExit(func() {
        runBranchFastForward(context.Background(), output, cat, &BranchFastForwardCmd{
            TableID: "db.events", BranchName: "staging", To: "feature", Yes: true,
        })
    })

    require.Zero(t, exit)
    require.NoError(t, output.lastErr)
    assert.Contains(t, output.message, "snapshot 200")
    require.NotNil(t, cat.committed)
    ref, ok := findSnapshotRef(cat.committed, "staging")
    require.True(t, ok)
    assert.Equal(t, int64(200), ref.SnapshotID)
    assert.Len(t, cat.updates, 1)
    // The CLI must pin both refs and not silently accept a changed source.
    assert.Len(t, cat.requirements, 3)
}

func TestRunBranchFastForwardNoop(t *testing.T) {
    tbl := branchFastForwardTestTable(t)
    cat := &branchTagCommitCatalog{tbl: tbl}
    output := &branchForwardCapture{}
    exit := captureBranchTagExit(func() {
        runBranchFastForward(context.Background(), output, cat, &BranchFastForwardCmd{
            TableID: "db.events", BranchName: "staging", To: "staging",
        })
    })

    require.Zero(t, exit)
    require.NoError(t, output.lastErr)
    assert.Contains(t, output.message, "already at snapshot 100")
    assert.Empty(t, cat.updates)
}

func TestRunBranchFastForwardRejectsNonDescendant(t *testing.T) {
    tbl := branchFastForwardTestTable(t)
    cat := &branchTagCommitCatalog{tbl: tbl}
    output := &branchForwardCapture{}
    exit := captureBranchTagExit(func() {
        runBranchFastForward(context.Background(), output, cat, &BranchFastForwardCmd{
            TableID: "db.events", BranchName: "feature", To: "staging", Yes: true,
        })
    })

    assert.Equal(t, 1, exit)
    require.Error(t, output.lastErr)
    assert.ErrorContains(t, output.lastErr, "not an ancestor")
    assert.Empty(t, cat.updates)
}

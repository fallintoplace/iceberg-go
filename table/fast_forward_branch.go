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
    "fmt"

    "github.com/apache/iceberg-go"
)

// FastForwardBranch advances the branch named from to the snapshot referenced
// by to, which may be a branch or a tag. If from already exists, its head must
// be an ancestor of to's head. A missing from branch is created.
//
// Fast-forwarding only changes a snapshot reference: it never rewrites data
// files or creates a snapshot. Existing retention settings on from are kept.
// Both references are pinned to their base values so a concurrent change to
// either reference rejects the commit rather than publishing an unchecked
// fast-forward. Use NewTransactionOnBranch(from) to avoid fencing an unrelated
// transaction branch.
func (t *Transaction) FastForwardBranch(from, to string) error {
    meta, err := t.txnMeta()
    if err != nil {
        return err
    }
    if from == "" || to == "" {
        return fmt.Errorf("%w: fast-forward requires non-empty from and to ref names", iceberg.ErrInvalidArgument)
    }

    source, ok := meta.refs[to]
    if !ok {
        return fmt.Errorf("%w: fast-forward source ref %q does not exist", iceberg.ErrInvalidArgument, to)
    }

    // A dangling source ref is invalid metadata, not a valid candidate to
    // create a branch at. Validate it even when the destination is absent.
    if _, err := meta.SnapshotByID(source.SnapshotID); err != nil {
        return fmt.Errorf("fast-forward source ref %q points to an unknown snapshot %d: %w",
            to, source.SnapshotID, err)
    }

    dest, exists := meta.refs[from]
    if exists {
        if dest.Type != BranchRef {
            return fmt.Errorf("%w: fast-forward destination %q is a %s, not a branch",
                iceberg.ErrInvalidArgument, from, dest.Type)
        }

        lookup := func(id int64) *Snapshot {
            snap, _ := meta.SnapshotByID(id)
            return snap
        }
        if !IsAncestorOf(source.SnapshotID, dest.SnapshotID, lookup) {
            return fmt.Errorf("%w: branch %q at snapshot %d is not an ancestor of ref %q at snapshot %d",
                iceberg.ErrInvalidArgument, from, dest.SnapshotID, to, source.SnapshotID)
        }
        if dest.SnapshotID == source.SnapshotID {
            // Still check transaction state: a committed or unusable
            // transaction must not appear to accept a new operation.
            return t.apply(nil, nil)
        }
    }

    // The builder does not retain existing ref policies for an ordinary
    // set-snapshot-ref update. This helper explicitly copies those values.
    update := meta.NewRetainingSnapshotRefUpdate(from, source.SnapshotID, BranchRef)

    // Always guard both refs, including absence of a newly created branch.
    // A source advance can invalidate the ancestry that was checked above;
    // an unrelated target advance must not be silently overwritten.
    requirements := []Requirement{
        AssertRefSnapshotID(from, t.baseRefSnapshotID(from)),
        AssertRefSnapshotID(to, t.baseRefSnapshotID(to)),
    }
    return t.applyPinned([]Update{update}, requirements, []string{from, to})
}

// FastForwardBranch is the one-commit convenience API for advancing a branch.
// To compose it with other table updates, use Transaction.FastForwardBranch.
func (t Table) FastForwardBranch(ctx context.Context, from, to string) (*Table, error) {
    tx, err := t.NewTransactionOnBranch(from)
    if err != nil {
        return nil, err
    }
    if err := tx.FastForwardBranch(from, to); err != nil {
        return nil, err
    }

    return tx.Commit(ctx)
}

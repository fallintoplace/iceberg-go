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

package table

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	"github.com/apache/iceberg-go"
)

// branchPromotionHeads resolves two existing branches in the staged table.
// Promotion does not accept snapshots created earlier in the same transaction:
// it must reason about two committed histories, not an in-flight snapshot.
func (t *Transaction) branchPromotionHeads(source string) (targetName string, target, sourceHead *Snapshot, err error) {
	meta, err := t.txnMeta()
	if err != nil {
		return "", nil, nil, err
	}
	if t.tbl == nil || t.tbl.Metadata() == nil {
		return "", nil, nil, fmt.Errorf("%w: cannot promote branches without base metadata", ErrInvalidMetadata)
	}
	targetName = t.branch
	if targetName == "" {
		targetName = MainBranch
	}
	if source == "" {
		return "", nil, nil, fmt.Errorf("%w: source branch name is empty", iceberg.ErrInvalidArgument)
	}

	heads := [2]string{targetName, source}
	resolved := [2]*Snapshot{}
	for i, name := range heads {
		ref, exists := meta.refs[name]
		if !exists {
			return "", nil, nil, fmt.Errorf("%w: branch %q does not exist", iceberg.ErrInvalidArgument, name)
		}
		if ref.Type != BranchRef {
			return "", nil, nil, fmt.Errorf("%w: ref %q is not a branch", iceberg.ErrInvalidArgument, name)
		}
		base := t.baseRefSnapshotID(name)
		if base == nil || *base != ref.SnapshotID {
			return "", nil, nil, fmt.Errorf("%w: branch %q has staged changes; use a fresh transaction", iceberg.ErrInvalidArgument, name)
		}
		snap, lookupErr := meta.SnapshotByID(ref.SnapshotID)
		if lookupErr != nil || snap == nil {
			return "", nil, nil, fmt.Errorf("%w: branch %q references missing snapshot %d", ErrInvalidMetadata, name, ref.SnapshotID)
		}
		resolved[i] = snap
	}
	return targetName, resolved[0], resolved[1], nil
}

// FastForwardBranch advances the transaction's target branch to the head of
// source, without creating a snapshot or changing source. The target must be
// an ancestor of source. Both branch heads are pinned to the base metadata,
// so a concurrent update to either branch fails the commit instead of silently
// moving the target to a stale or unrelated snapshot.
//
// Create a transaction with NewTransactionOnBranch to fast-forward a branch
// other than main. Ref retention properties on the target are preserved.
func (t *Transaction) FastForwardBranch(source string) error {
	targetName, target, sourceHead, err := t.branchPromotionHeads(source)
	if err != nil {
		return err
	}
	if sourceHead.SnapshotID == target.SnapshotID {
		return nil
	}

	lookup := func(id int64) *Snapshot {
		snap, lookupErr := t.meta.SnapshotByID(id)
		if lookupErr != nil {
			return nil
		}
		return snap
	}
	if !IsAncestorOf(sourceHead.SnapshotID, target.SnapshotID, lookup) {
		return fmt.Errorf("%w: branch %q head %d is not an ancestor of %q head %d",
			iceberg.ErrInvalidArgument, targetName, target.SnapshotID, source, sourceHead.SnapshotID)
	}

	update := t.meta.NewRetainingSnapshotRefUpdate(targetName, sourceHead.SnapshotID, BranchRef)
	return t.applyPinned(
		[]Update{update},
		[]Requirement{
			AssertRefSnapshotID(targetName, t.baseRefSnapshotID(targetName)),
			AssertRefSnapshotID(source, t.baseRefSnapshotID(source)),
		},
		[]string{targetName, source},
	)
}

// CherryPickAppendBranch publishes all append snapshots on source since its
// most recent common ancestor with the transaction's target branch. It stages
// one append snapshot containing the source branch's newly added data files;
// existing Parquet data is reused, not copied. Commits from either branch
// after this transaction was created cause an optimistic-concurrency failure.
//
// Unlike a full Iceberg cherry-pick, overwrite, delete, and rewrite snapshots
// are deliberately unsupported. A source history containing any such snapshot
// is rejected before staging a write. Use FastForwardBranch when the target
// has not diverged and preserving the source snapshot IDs is preferable.
//
// A second publication of the same files is rejected by AddDataFiles' live-file
// duplicate check. The new snapshot's summary records source-snapshot-id as
// the source branch HEAD, including when several appends are consolidated.
func (t *Transaction) CherryPickAppendBranch(ctx context.Context, source string, snapshotProps iceberg.Properties) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	targetName, target, sourceHead, err := t.branchPromotionHeads(source)
	if err != nil {
		return err
	}
	if sourceHead.SnapshotID == target.SnapshotID {
		return nil
	}

	lookup := func(id int64) *Snapshot {
		snap, lookupErr := t.meta.SnapshotByID(id)
		if lookupErr != nil {
			return nil
		}
		return snap
	}
	targetHistory, targetComplete := AncestorsOfChecked(target.SnapshotID, lookup)
	sourceHistory, sourceComplete := AncestorsOfChecked(sourceHead.SnapshotID, lookup)
	if !targetComplete || !sourceComplete {
		return fmt.Errorf("%w: cannot cherry-pick branches with missing or cyclic snapshot ancestry", iceberg.ErrInvalidArgument)
	}

	targetIDs := make(map[int64]struct{}, len(targetHistory))
	for _, snap := range targetHistory {
		targetIDs[snap.SnapshotID] = struct{}{}
	}
	commonIndex := -1
	for i, snap := range sourceHistory {
		if _, ok := targetIDs[snap.SnapshotID]; ok {
			commonIndex = i
			break
		}
	}
	if commonIndex < 0 {
		return fmt.Errorf("%w: branches %q and %q do not have a common snapshot", iceberg.ErrInvalidArgument, targetName, source)
	}
	if commonIndex == 0 {
		return nil // The source is already an ancestor of the target.
	}

	for _, snap := range sourceHistory[:commonIndex] {
		if snap.Summary == nil || snap.Summary.Operation != OpAppend {
			return fmt.Errorf("%w: snapshot %d on branch %q is not an append", ErrInvalidOperation, snap.SnapshotID, source)
		}
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	files := make([]iceberg.DataFile, 0)
	seenPaths := make(map[string]struct{})
	// Oldest first is deterministic and preserves the source's append order.
	for i := commonIndex - 1; i >= 0; i-- {
		snap := sourceHistory[i]
		if err := ctx.Err(); err != nil {
			return err
		}
		manifests, err := snap.Manifests(fs)
		if err != nil {
			return fmt.Errorf("read snapshot %d manifest list: %w", snap.SnapshotID, err)
		}
		for _, manifest := range manifests {
			if manifest.SnapshotID() != snap.SnapshotID {
				continue // Inherited manifests do not describe this append.
			}
			if manifest.ManifestContent() != iceberg.ManifestContentData {
				return fmt.Errorf("%w: append snapshot %d added a delete manifest", ErrInvalidOperation, snap.SnapshotID)
			}
			for entry, readErr := range manifest.Entries(fs, false) {
				if readErr != nil {
					return fmt.Errorf("read snapshot %d manifest %q: %w", snap.SnapshotID, manifest.FilePath(), readErr)
				}
				if entry == nil || entry.SnapshotID() != snap.SnapshotID {
					continue
				}
				if entry.Status() == iceberg.EntryStatusDELETED {
					return fmt.Errorf("%w: append snapshot %d removed a data file", ErrInvalidOperation, snap.SnapshotID)
				}
				if entry.Status() != iceberg.EntryStatusADDED {
					continue
				}
				df := entry.DataFile()
				if df == nil || df.ContentType() != iceberg.EntryContentData || df.FilePath() == "" {
					return fmt.Errorf("%w: invalid added data file in snapshot %d", ErrInvalidMetadata, snap.SnapshotID)
				}
				if _, exists := seenPaths[df.FilePath()]; exists {
					return fmt.Errorf("%w: file %q is added more than once in the source history", iceberg.ErrInvalidArgument, df.FilePath())
				}
				seenPaths[df.FilePath()] = struct{}{}
				files = append(files, df)
			}
		}
	}
	if len(files) == 0 {
		return nil
	}

	// Register these before building the snapshot. Append producers ordinarily
	// rebase on an updated target head, but a replay must fail instead: another
	// cherry-pick may already have published the same data files.
	if err := t.AssertRefSnapshotID(targetName); err != nil {
		return err
	}
	if err := t.AssertRefSnapshotID(source); err != nil {
		return err
	}

	props := maps.Clone(snapshotProps)
	if props == nil {
		props = make(iceberg.Properties, 1)
	}
	props["source-snapshot-id"] = strconv.FormatInt(sourceHead.SnapshotID, 10)
	return t.AddDataFiles(ctx, files, props, WithoutAutoNameMapping())
}

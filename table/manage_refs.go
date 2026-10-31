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
	"strings"

	"github.com/apache/iceberg-go"
)

// CreateBranch stages a new branch pointing at an existing snapshot. No data
// files are rewritten. Retention can be configured using WithMaxRefAgeMs,
// WithMaxSnapshotAgeMs, and WithMinSnapshotsToKeep.
//
// Branch names share a namespace with tags. A branch cannot replace an existing
// ref, including one created earlier in the same transaction. Creation is
// guarded by a ref-absence requirement across commit retries.
func (t *Transaction) CreateBranch(name string, snapshotID int64, opts ...setSnapshotRefOption) error {
	return t.createSnapshotRef(name, snapshotID, BranchRef, opts...)
}

// CreateTag stages a new tag pointing at an existing snapshot. Tags support
// WithMaxRefAgeMs, but not branch-specific snapshot retention options.
func (t *Transaction) CreateTag(name string, snapshotID int64, opts ...setSnapshotRefOption) error {
	return t.createSnapshotRef(name, snapshotID, TagRef, opts...)
}

func validateSnapshotRefName(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: snapshot ref name must not be empty", iceberg.ErrInvalidArgument)
	}

	return nil
}

func applySnapshotRefOptions(name string, ref *SnapshotRef, opts []setSnapshotRefOption) error {
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("%w: nil snapshot ref option", iceberg.ErrInvalidArgument)
		}
		if err := opt(ref); err != nil {
			return err
		}
	}
	if name == MainBranch && ref.MaxRefAgeMs != nil {
		return fmt.Errorf("%w: the main branch cannot have a max reference age", iceberg.ErrInvalidArgument)
	}
	if err := ref.validate(); err != nil {
		return fmt.Errorf("%w: invalid snapshot ref: %w", iceberg.ErrInvalidArgument, err)
	}

	return nil
}

// refUpdate retains all of a ref's existing retention settings when moving it.
// The underlying metadata update takes zero for an unset optional field.
func refUpdate(name string, ref SnapshotRef) Update {
	var maxRefAgeMs, maxSnapshotAgeMs int64
	var minSnapshotsToKeep int
	if ref.MaxRefAgeMs != nil {
		maxRefAgeMs = *ref.MaxRefAgeMs
	}
	if ref.MaxSnapshotAgeMs != nil {
		maxSnapshotAgeMs = *ref.MaxSnapshotAgeMs
	}
	if ref.MinSnapshotsToKeep != nil {
		minSnapshotsToKeep = *ref.MinSnapshotsToKeep
	}

	return NewSetSnapshotRefUpdate(name, ref.SnapshotID, ref.Type,
		maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep)
}

// refChangeRequirements pins a ref to its base-table snapshot id. An absent
// staged ref requires absence in the catalog. A present ref created earlier in
// this transaction already has an absence requirement from CreateBranch/Tag,
// so asserting the new staged id against the base table would be incorrect.
func (t *Transaction) refChangeRequirements(name string, stagedExists bool) []Requirement {
	if id := t.baseRefSnapshotID(name); id != nil {
		return []Requirement{AssertRefSnapshotID(name, id)}
	}
	if !stagedExists {
		return []Requirement{AssertRefSnapshotID(name, nil)}
	}

	return nil
}

func (t *Transaction) createSnapshotRef(name string, snapshotID int64, refType RefType, opts ...setSnapshotRefOption) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := validateSnapshotRefName(name); err != nil {
		return err
	}
	if name == MainBranch && refType != BranchRef {
		return fmt.Errorf("%w: main must be a branch", iceberg.ErrInvalidArgument)
	}
	if _, exists := meta.refs[name]; exists {
		return fmt.Errorf("%w: snapshot ref %q already exists", iceberg.ErrInvalidArgument, name)
	}
	if t.baseRefSnapshotID(name) != nil {
		return fmt.Errorf("%w: snapshot ref %q existed at transaction start", iceberg.ErrInvalidArgument, name)
	}
	if _, err := meta.SnapshotByID(snapshotID); err != nil {
		return fmt.Errorf("%w: cannot create ref %q at snapshot %d: %w",
			iceberg.ErrInvalidArgument, name, snapshotID, err)
	}

	ref := SnapshotRef{SnapshotID: snapshotID, Type: refType}
	if err := applySnapshotRefOptions(name, &ref, opts); err != nil {
		return err
	}

	return t.applyPinned([]Update{refUpdate(name, ref)},
		[]Requirement{AssertRefSnapshotID(name, nil)}, []string{name})
}

// FastForwardBranch moves target to source's snapshot when target's current
// snapshot is an ancestor of source's. Source may be a branch or a tag and is
// not modified. A missing target branch is created at the source snapshot.
// Existing target retention is preserved. Both refs are pinned so neither a
// concurrent source advance nor a concurrent target advance is silently lost.
func (t *Transaction) FastForwardBranch(target, source string) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := validateSnapshotRefName(target); err != nil {
		return err
	}
	if err := validateSnapshotRefName(source); err != nil {
		return err
	}

	sourceRef, ok := meta.refs[source]
	if !ok {
		return fmt.Errorf("%w: source ref %q does not exist", iceberg.ErrInvalidArgument, source)
	}
	if _, err := meta.SnapshotByID(sourceRef.SnapshotID); err != nil {
		return fmt.Errorf("source ref %q points to an invalid snapshot: %w", source, err)
	}
	targetRef, exists := meta.refs[target]
	if exists && targetRef.Type != BranchRef {
		return fmt.Errorf("%w: target ref %q is not a branch", iceberg.ErrInvalidArgument, target)
	}
	if target == source || (exists && targetRef.SnapshotID == sourceRef.SnapshotID) {
		return nil
	}

	if exists {
		lookup := func(id int64) *Snapshot {
			snapshot, _ := meta.SnapshotByID(id)

			return snapshot
		}
		if !IsAncestorOf(sourceRef.SnapshotID, targetRef.SnapshotID, lookup) {
			return fmt.Errorf("%w: branch %q snapshot %d is not an ancestor of %q snapshot %d",
				iceberg.ErrInvalidArgument, target, targetRef.SnapshotID, source, sourceRef.SnapshotID)
		}
	} else {
		targetRef = SnapshotRef{Type: BranchRef}
	}
	targetRef.SnapshotID = sourceRef.SnapshotID

	requirements := t.refChangeRequirements(target, exists)
	if source != target {
		requirements = append(requirements, t.refChangeRequirements(source, true)...)
	}

	return t.applyPinned([]Update{refUpdate(target, targetRef)}, requirements, []string{target, source})
}

// RenameBranch atomically moves a branch's name while preserving its snapshot
// and retention policy. The main branch cannot be renamed and the destination
// must not already be a branch or a tag.
func (t *Transaction) RenameBranch(oldName, newName string) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := validateSnapshotRefName(oldName); err != nil {
		return err
	}
	if err := validateSnapshotRefName(newName); err != nil {
		return err
	}
	if oldName == MainBranch || newName == MainBranch {
		return fmt.Errorf("%w: cannot rename the main branch", iceberg.ErrInvalidArgument)
	}
	ref, ok := meta.refs[oldName]
	if !ok || ref.Type != BranchRef {
		return fmt.Errorf("%w: branch %q does not exist", iceberg.ErrInvalidArgument, oldName)
	}
	if oldName == newName {
		return nil
	}
	if _, exists := meta.refs[newName]; exists {
		return fmt.Errorf("%w: ref %q already exists", iceberg.ErrInvalidArgument, newName)
	}
	if t.baseRefSnapshotID(newName) != nil {
		return fmt.Errorf("%w: ref %q existed at transaction start", iceberg.ErrInvalidArgument, newName)
	}

	requirements := t.refChangeRequirements(oldName, true)
	requirements = append(requirements, AssertRefSnapshotID(newName, nil))

	return t.applyPinned([]Update{refUpdate(newName, ref), NewRemoveSnapshotRefUpdate(oldName)},
		requirements, []string{oldName, newName})
}

// ReplaceTag moves an existing tag to another snapshot while retaining its
// reference-age policy. Unlike FastForwardBranch, tags have no ancestry rule.
func (t *Transaction) ReplaceTag(name string, snapshotID int64) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := validateSnapshotRefName(name); err != nil {
		return err
	}
	ref, ok := meta.refs[name]
	if !ok || ref.Type != TagRef {
		return fmt.Errorf("%w: tag %q does not exist", iceberg.ErrInvalidArgument, name)
	}
	if ref.SnapshotID == snapshotID {
		return nil
	}
	if _, err := meta.SnapshotByID(snapshotID); err != nil {
		return fmt.Errorf("%w: cannot replace tag %q with snapshot %d: %w",
			iceberg.ErrInvalidArgument, name, snapshotID, err)
	}
	ref.SnapshotID = snapshotID

	return t.applyPinned([]Update{refUpdate(name, ref)},
		t.refChangeRequirements(name, true), []string{name})
}

// SetRefRetention updates a branch or tag's retention policy in place. It
// preserves fields not mentioned by opts and does not move the ref.
func (t *Transaction) SetRefRetention(name string, opts ...setSnapshotRefOption) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := validateSnapshotRefName(name); err != nil {
		return err
	}
	ref, ok := meta.refs[name]
	if !ok {
		return fmt.Errorf("%w: ref %q does not exist", iceberg.ErrInvalidArgument, name)
	}
	if len(opts) == 0 {
		return nil
	}
	updated := ref
	if err := applySnapshotRefOptions(name, &updated, opts); err != nil {
		return err
	}
	if ref.Equals(updated) {
		return nil
	}

	return t.applyPinned([]Update{refUpdate(name, updated)},
		t.refChangeRequirements(name, true), []string{name})
}

// RemoveBranch removes a non-main branch without deleting snapshots or files.
func (t *Transaction) RemoveBranch(name string) error {
	return t.removeSnapshotRef(name, BranchRef)
}

// RemoveTag removes a tag without deleting snapshots or files.
func (t *Transaction) RemoveTag(name string) error {
	return t.removeSnapshotRef(name, TagRef)
}

func (t *Transaction) removeSnapshotRef(name string, refType RefType) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := validateSnapshotRefName(name); err != nil {
		return err
	}
	if name == MainBranch {
		return fmt.Errorf("%w: cannot remove the main branch", iceberg.ErrInvalidArgument)
	}
	ref, ok := meta.refs[name]
	if !ok || ref.Type != refType {
		return fmt.Errorf("%w: %s %q does not exist", iceberg.ErrInvalidArgument, refType, name)
	}

	return t.applyPinned([]Update{NewRemoveSnapshotRefUpdate(name)},
		t.refChangeRequirements(name, true), []string{name})
}

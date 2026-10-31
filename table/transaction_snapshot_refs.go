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
	"unicode"

	"github.com/apache/iceberg-go"
)

// SnapshotRefOption configures an Iceberg branch or tag retention policy.
// The existing WithMaxRefAgeMs, WithMaxSnapshotAgeMs and
// WithMinSnapshotsToKeep options can be used with these transaction methods.
type SnapshotRefOption = setSnapshotRefOption

// WithoutMaxRefAgeMs removes a reference-specific age limit, falling back to
// the table's reference retention policy.
func WithoutMaxRefAgeMs() SnapshotRefOption {
	return func(ref *SnapshotRef) error {
		ref.MaxRefAgeMs = nil
		return nil
	}
}

// WithoutMaxSnapshotAgeMs removes a branch-specific snapshot age limit.
func WithoutMaxSnapshotAgeMs() SnapshotRefOption {
	return func(ref *SnapshotRef) error {
		ref.MaxSnapshotAgeMs = nil
		return nil
	}
}

// WithoutMinSnapshotsToKeep removes a branch-specific minimum snapshot count.
func WithoutMinSnapshotsToKeep() SnapshotRefOption {
	return func(ref *SnapshotRef) error {
		ref.MinSnapshotsToKeep = nil
		return nil
	}
}

func validateSnapshotRefName(name string) error {
	if name == "" || strings.TrimSpace(name) != name || name == "." || name == ".." ||
		strings.ContainsFunc(name, unicode.IsControl) {
		return fmt.Errorf("%w: invalid snapshot reference name %q", iceberg.ErrInvalidArgument, name)
	}
	return nil
}

func applySnapshotRefOptions(name string, ref *SnapshotRef, opts []SnapshotRefOption) error {
	for _, opt := range opts {
		if opt == nil {
			return fmt.Errorf("%w: nil snapshot ref option", iceberg.ErrInvalidArgument)
		}
		if err := opt(ref); err != nil {
			return fmt.Errorf("%w: invalid retention for %q: %w", iceberg.ErrInvalidArgument, name, err)
		}
	}
	if name == MainBranch && ref.MaxRefAgeMs != nil {
		return fmt.Errorf("%w: the main branch cannot have max-ref-age-ms", iceberg.ErrInvalidArgument)
	}
	if err := ref.validate(); err != nil {
		return fmt.Errorf("%w: invalid snapshot reference %q: %w", iceberg.ErrInvalidArgument, name, err)
	}
	return nil
}

func snapshotRefUpdate(name string, ref SnapshotRef) Update {
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

// mutateSnapshotRef stages a ref operation and its base-state assertion under
// the transaction lock. Pinning is published only after applyLocked succeeds.
// This keeps ref changes safe from the commit retry loop's normal rebasing.
func (t *Transaction) mutateSnapshotRef(name string, stage func(*MetadataBuilder) (Update, error)) error {
	if err := t.checkNotNil(); err != nil {
		return err
	}
	t.mx.Lock()
	defer t.mx.Unlock()

	meta, err := t.txnMeta()
	if err != nil {
		return err
	}
	if err := t.checkUsable(); err != nil {
		return err
	}
	if err := validateSnapshotRefName(name); err != nil {
		return err
	}

	update, err := stage(meta)
	if err != nil {
		return err
	}
	if update == nil {
		return nil
	}

	// The requirement must refer to the catalog state the transaction read,
	// not to a staged ref created or updated earlier in this transaction.
	req := AssertRefSnapshotID(name, t.baseRefSnapshotID(name))
	if err := t.applyLocked([]Update{update}, []Requirement{req}); err != nil {
		return err
	}

	if t.pinnedRefs == nil {
		t.pinnedRefs = make(map[string]struct{})
	}
	t.pinnedRefs[name] = struct{}{}
	return nil
}

func (t *Transaction) createSnapshotRef(name string, snapshotID int64, kind RefType, opts ...SnapshotRefOption) error {
	return t.mutateSnapshotRef(name, func(meta *MetadataBuilder) (Update, error) {
		if name == MainBranch {
			return nil, fmt.Errorf("%w: cannot create the reserved main branch", iceberg.ErrInvalidArgument)
		}
		if _, exists := meta.refs[name]; exists {
			return nil, fmt.Errorf("%w: snapshot ref %q already exists", iceberg.ErrInvalidArgument, name)
		}
		// A remove-then-create is not a new ref relative to the base catalog.
		// Require true absence in both base and staged metadata.
		if t.baseRefSnapshotID(name) != nil {
			return nil, fmt.Errorf("%w: snapshot ref %q existed at transaction start", iceberg.ErrInvalidArgument, name)
		}
		if _, err := meta.SnapshotByID(snapshotID); err != nil {
			return nil, fmt.Errorf("%w: cannot create ref %q at snapshot %d: %w",
				iceberg.ErrInvalidArgument, name, snapshotID, err)
		}

		ref := SnapshotRef{SnapshotID: snapshotID, Type: kind}
		if err := applySnapshotRefOptions(name, &ref, opts); err != nil {
			return nil, err
		}
		return snapshotRefUpdate(name, ref), nil
	})
}

// CreateBranch stages a new branch at an existing snapshot. The branch must not
// already exist. Its absence is asserted at commit, preventing concurrent
// creators from overwriting one another. Commit the transaction to persist it.
//
// Example: tx.CreateBranch("audit", snapshotID, WithMinSnapshotsToKeep(5)).
func (t *Transaction) CreateBranch(name string, snapshotID int64, opts ...SnapshotRefOption) error {
	return t.createSnapshotRef(name, snapshotID, BranchRef, opts...)
}

// CreateTag stages a new tag at an existing snapshot. Tags cannot be moved by
// these APIs; retention changes leave their snapshot ID intact. The tag must
// not already exist, including as a branch.
func (t *Transaction) CreateTag(name string, snapshotID int64, opts ...SnapshotRefOption) error {
	return t.createSnapshotRef(name, snapshotID, TagRef, opts...)
}

func (t *Transaction) removeSnapshotRef(name string, expected RefType) error {
	return t.mutateSnapshotRef(name, func(meta *MetadataBuilder) (Update, error) {
		if name == MainBranch {
			return nil, fmt.Errorf("%w: cannot delete the main branch", iceberg.ErrInvalidArgument)
		}
		ref, ok := meta.refs[name]
		if !ok {
			return nil, fmt.Errorf("%w: snapshot ref %q does not exist", iceberg.ErrInvalidArgument, name)
		}
		if ref.Type != expected {
			return nil, fmt.Errorf("%w: ref %q is a %s, not a %s",
				iceberg.ErrInvalidArgument, name, ref.Type, expected)
		}
		return NewRemoveSnapshotRefUpdate(name), nil
	})
}

// RemoveBranch stages deletion of a non-main branch. An optimistic concurrency
// assertion protects its observed head from a concurrent move or append.
func (t *Transaction) RemoveBranch(name string) error {
	return t.removeSnapshotRef(name, BranchRef)
}

// RemoveTag stages deletion of a tag without affecting the pointed-to snapshot.
func (t *Transaction) RemoveTag(name string) error {
	return t.removeSnapshotRef(name, TagRef)
}

// UpdateSnapshotRefRetention changes only the supplied retention fields.
// Unspecified settings remain unchanged; Without* options clear overrides
// and restore table-level defaults. This never moves a branch or tag, and
// the observed snapshot head is fenced against concurrent updates.
func (t *Transaction) UpdateSnapshotRefRetention(name string, opts ...SnapshotRefOption) error {
	return t.mutateSnapshotRef(name, func(meta *MetadataBuilder) (Update, error) {
		ref, ok := meta.refs[name]
		if !ok {
			return nil, fmt.Errorf("%w: snapshot ref %q does not exist", iceberg.ErrInvalidArgument, name)
		}
		next := cloneSnapshotRef(ref)
		if err := applySnapshotRefOptions(name, &next, opts); err != nil {
			return nil, err
		}
		if ref.Equals(next) {
			return nil, nil
		}
		return snapshotRefUpdate(name, next), nil
	})
}

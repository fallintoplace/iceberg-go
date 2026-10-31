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
	"strings"
	"unicode"

	"github.com/apache/iceberg-go"
)

// FastForwardBranch advances targetBranch to the snapshot referenced by sourceRef.
// sourceRef may name a branch or a tag; targetBranch must name a branch or
// be absent. An absent target is created with default retention settings.
// An existing target must be an ancestor of the source snapshot; divergent
// histories cannot be fast-forwarded. When the target already points to the
// source snapshot, the method returns without committing.
//
// The target's retention settings are preserved. Both the source and target
// snapshot IDs are fenced with catalog commit requirements, so concurrent
// advances of either ref fail rather than silently retargeting the operation.
// This changes only table metadata; it does not rewrite data or create a
// new snapshot.
func (t Table) FastForwardBranch(ctx context.Context, targetBranch, sourceRef string) (*Table, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", iceberg.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.metadata == nil {
		return nil, fmt.Errorf("%w: table metadata is nil", ErrInvalidMetadata)
	}
	if !validFastForwardRefName(targetBranch) || !validFastForwardRefName(sourceRef) {
		return nil, fmt.Errorf("%w: branch and source reference names must be non-empty and valid", iceberg.ErrInvalidArgument)
	}

	var source, target SnapshotRef
	var sourceExists, targetExists bool
	for name, ref := range t.metadata.Refs() {
		if name == sourceRef {
			source, sourceExists = ref, true
		}
		if name == targetBranch {
			target, targetExists = ref, true
		}
	}

	if !sourceExists {
		return nil, fmt.Errorf("%w: source ref %q does not exist", iceberg.ErrInvalidArgument, sourceRef)
	}
	if targetExists && target.Type != BranchRef {
		return nil, fmt.Errorf("%w: ref %q is a %s, not a branch", iceberg.ErrInvalidArgument, targetBranch, target.Type)
	}
	if t.metadata.SnapshotByID(source.SnapshotID) == nil {
		return nil, fmt.Errorf("%w: source ref %q points to missing snapshot %d", ErrSnapshotNotFound, sourceRef, source.SnapshotID)
	}
	if targetExists {
		if t.metadata.SnapshotByID(target.SnapshotID) == nil {
			return nil, fmt.Errorf("%w: target branch %q points to missing snapshot %d", ErrSnapshotNotFound, targetBranch, target.SnapshotID)
		}
		if target.SnapshotID == source.SnapshotID {
			copy := t
			return &copy, nil
		}
		if !IsAncestorOf(source.SnapshotID, target.SnapshotID, t.metadata.SnapshotByID) {
			return nil, fmt.Errorf("%w: target branch %q at snapshot %d is not an ancestor of %q at snapshot %d",
				iceberg.ErrInvalidArgument, targetBranch, target.SnapshotID, sourceRef, source.SnapshotID)
		}
	}

	tx, err := t.NewTransactionOnBranch(targetBranch)
	if err != nil {
		return nil, err
	}
	// Pin both refs before staging the metadata update. A concurrent advance
	// must not be hidden by Transaction.Commit's normal refresh-and-replay.
	if err := tx.AssertRefSnapshotID(targetBranch); err != nil {
		return nil, err
	}
	if sourceRef != targetBranch {
		if err := tx.AssertRefSnapshotID(sourceRef); err != nil {
			return nil, err
		}
	}
	update := tx.meta.NewRetainingSnapshotRefUpdate(targetBranch, source.SnapshotID, BranchRef)
	if err := tx.apply([]Update{update}, nil); err != nil {
		return nil, err
	}

	return tx.Commit(ctx)
}

func validFastForwardRefName(name string) bool {
	return name != "" && strings.TrimSpace(name) == name && name != "." && name != ".." &&
		!strings.ContainsFunc(name, unicode.IsControl)
}

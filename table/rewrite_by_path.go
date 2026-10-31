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
	"runtime"

	"github.com/apache/iceberg-go"
)

// NewRewriteFromPaths builds a data-file rewrite from existing file paths.
// It resolves removed files against the transaction's target branch and reads
// the replacement Parquet footers to construct their data-file metadata.
//
// Unlike ReplaceDataFilesByPath, which performs an overwrite, this operation
// produces an Iceberg replace snapshot. The caller must ensure that the new
// files contain exactly the same live rows as the removed files, including
// the effects of delete files and, for v3 tables, row lineage. File metadata
// alone cannot establish that equivalence.
//
// The returned builder can be configured with DataSequenceNumber or additional
// delete-file changes. Call RewriteFiles.Commit to stage the snapshot, then
// Transaction.Commit to persist it. The rewrite validator detects concurrent
// delete files that would make the replacement unsafe.
//
// Both path lists must be non-empty, unique, and disjoint. Removed paths must
// be live data files in the target branch; added paths must not already be
// referenced there. The method may stage the table's default name mapping
// before returning the builder, but does not stage a snapshot.
func (t *Transaction) NewRewriteFromPaths(
	ctx context.Context,
	filesToDelete, filesToAdd []string,
	snapshotProps iceberg.Properties,
) (*RewriteFiles, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}
	if len(filesToDelete) == 0 || len(filesToAdd) == 0 {
		return nil, fmt.Errorf("%w: a data-file rewrite requires both removed and added files", ErrInvalidOperation)
	}

	toDelete := make(map[string]struct{}, len(filesToDelete))
	for _, path := range filesToDelete {
		if path == "" {
			return nil, fmt.Errorf("%w: removed file paths must be non-empty", ErrInvalidOperation)
		}
		if _, exists := toDelete[path]; exists {
			return nil, fmt.Errorf("%w: duplicate removed file path %q", ErrInvalidOperation, path)
		}
		toDelete[path] = struct{}{}
	}

	toAdd := make(map[string]struct{}, len(filesToAdd))
	for _, path := range filesToAdd {
		if path == "" {
			return nil, fmt.Errorf("%w: added file paths must be non-empty", ErrInvalidOperation)
		}
		if _, exists := toAdd[path]; exists {
			return nil, fmt.Errorf("%w: duplicate added file path %q", ErrInvalidOperation, path)
		}
		if _, exists := toDelete[path]; exists {
			return nil, fmt.Errorf("%w: cannot remove and add the same file %q", ErrInvalidOperation, path)
		}
		toAdd[path] = struct{}{}
	}

	snapshot := t.planningSnapshot(meta)
	if snapshot == nil {
		return nil, fmt.Errorf("%w: cannot rewrite files in a table without an existing snapshot", ErrInvalidOperation)
	}

	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := requireWriteFileIO(fs); err != nil {
		return nil, err
	}

	removed := make([]iceberg.DataFile, 0, len(filesToDelete))
	for df, err := range snapshot.dataFiles(fs, nil) {
		if err != nil {
			return nil, err
		}
		if _, exists := toAdd[df.FilePath()]; exists {
			return nil, fmt.Errorf("%w: added file %q is already referenced by the table", ErrInvalidOperation, df.FilePath())
		}
		if _, exists := toDelete[df.FilePath()]; exists {
			removed = append(removed, df)
		}
	}
	if len(removed) != len(filesToDelete) {
		return nil, fmt.Errorf("%w: all removed files must be live in the target branch", ErrInvalidOperation)
	}

	if err := t.ensureNameMapping(); err != nil {
		return nil, err
	}
	// ensureNameMapping may have replaced the staged metadata builder.
	meta, err = t.txnMeta()
	if err != nil {
		return nil, err
	}

	added, err := filesToDataFiles(ctx, fs, meta, filesToAdd, runtime.GOMAXPROCS(0))
	if err != nil {
		return nil, err
	}

	rewrite := t.NewRewrite(snapshotProps)
	for _, df := range removed {
		rewrite.DeleteFile(df)
	}
	for _, df := range added {
		rewrite.AddDataFile(df)
	}
	return rewrite, nil
}

// RewriteDataFilesByPath stages an Iceberg replace snapshot for a
// data-preserving rewrite of files identified by their storage paths.
// Call Transaction.Commit to persist it.
//
// This is the single-step counterpart of NewRewriteFromPaths. Use the builder
// instead if the rewrite also needs DataSequenceNumber or delete-file changes.
// As with RewriteFiles, verifying that the replacement preserves live rows and
// v3 row lineage is the caller's responsibility.
func (t *Transaction) RewriteDataFilesByPath(
	ctx context.Context,
	filesToDelete, filesToAdd []string,
	snapshotProps iceberg.Properties,
) error {
	rewrite, err := t.NewRewriteFromPaths(ctx, filesToDelete, filesToAdd, snapshotProps)
	if err != nil {
		return err
	}
	return rewrite.Commit(ctx)
}

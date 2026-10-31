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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// SnapshotFileDiff describes the change in live content files between two
// snapshots. Each slice is ordered by file path. DeleteFiles includes position
// deletes, equality deletes, and deletion vectors.
//
// This is a file-level comparison, not a row-level changelog. For example,
// replacing a Parquet data file during compaction produces an added and a
// removed file even if the rows have not changed.
type SnapshotFileDiff struct {
	AddedDataFiles     []iceberg.DataFile
	RemovedDataFiles   []iceberg.DataFile
	AddedDeleteFiles   []iceberg.DataFile
	RemovedDeleteFiles []iceberg.DataFile
}

// DiffSnapshotFiles compares the *live* data and delete files in two retained
// snapshots. Both IDs must exist in the table metadata. The snapshots need not
// be on the same ancestry chain, so this can also compare branches or tags.
//
// It reads the manifests of each snapshot and ignores DELETED entries, which
// are tombstones rather than live files. File identity is the immutable file
// path and content type; a file present in both snapshots is not reported as
// changed. Results are ordered by file path to be reproducible. This method
// neither reads data rows nor mutates the table, and requires memory
// proportional to the number of live files across the two snapshots.
//
// The provided context is checked between manifest entries. Existing manifest
// readers may still use a background context for an individual I/O operation.
func (t Table) DiffSnapshotFiles(ctx context.Context, fromID, toID int64) (SnapshotFileDiff, error) {
	var zero SnapshotFileDiff
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	from := t.SnapshotByID(fromID)
	if from == nil {
		return zero, fmt.Errorf("%w: from snapshot %d", ErrSnapshotNotFound, fromID)
	}
	to := t.SnapshotByID(toID)
	if to == nil {
		return zero, fmt.Errorf("%w: to snapshot %d", ErrSnapshotNotFound, toID)
	}
	if fromID == toID {
		return zero, nil
	}

	fs, err := t.FS(ctx)
	if err != nil {
		return zero, fmt.Errorf("open snapshot file IO: %w", err)
	}
	if fs == nil {
		return zero, fmt.Errorf("%w: snapshot file IO is nil", ErrInvalidMetadata)
	}

	before, err := collectSnapshotLiveFiles(ctx, *from, fs)
	if err != nil {
		return zero, err
	}
	after, err := collectSnapshotLiveFiles(ctx, *to, fs)
	if err != nil {
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	var diff SnapshotFileDiff
	for path, file := range after {
		previous, ok := before[path]
		if ok && previous.ContentType() == file.ContentType() {
			continue
		}
		if err := appendSnapshotFileChange(file, &diff.AddedDataFiles, &diff.AddedDeleteFiles); err != nil {
			return zero, err
		}
	}
	for path, file := range before {
		next, ok := after[path]
		if ok && next.ContentType() == file.ContentType() {
			continue
		}
		if err := appendSnapshotFileChange(file, &diff.RemovedDataFiles, &diff.RemovedDeleteFiles); err != nil {
			return zero, err
		}
	}

	sortSnapshotFileDiff(diff.AddedDataFiles)
	sortSnapshotFileDiff(diff.RemovedDataFiles)
	sortSnapshotFileDiff(diff.AddedDeleteFiles)
	sortSnapshotFileDiff(diff.RemovedDeleteFiles)

	return diff, nil
}

func collectSnapshotLiveFiles(ctx context.Context, snap Snapshot, fs iceio.IO) (map[string]iceberg.DataFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	files := make(map[string]iceberg.DataFile)
	for file, err := range snap.dataFiles(fs, nil) {
		if err != nil {
			return nil, fmt.Errorf("read live files for snapshot %d: %w", snap.SnapshotID, err)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if file == nil || file.FilePath() == "" {
			return nil, fmt.Errorf("%w: snapshot %d has a live file without a path", ErrInvalidMetadata, snap.SnapshotID)
		}
		switch file.ContentType() {
		case iceberg.EntryContentData, iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
		default:
			return nil, fmt.Errorf("%w: snapshot %d has unsupported file content %v at %q",
				ErrInvalidMetadata, snap.SnapshotID, file.ContentType(), file.FilePath())
		}
		if _, exists := files[file.FilePath()]; exists {
			return nil, fmt.Errorf("%w: snapshot %d references live file %q more than once",
				ErrInvalidMetadata, snap.SnapshotID, file.FilePath())
		}
		files[file.FilePath()] = file
	}
	return files, ctx.Err()
}

func appendSnapshotFileChange(file iceberg.DataFile, data, deletes *[]iceberg.DataFile) error {
	switch file.ContentType() {
	case iceberg.EntryContentData:
		*data = append(*data, file)
	case iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
		*deletes = append(*deletes, file)
	default:
		return fmt.Errorf("%w: unsupported content type %v", ErrInvalidMetadata, file.ContentType())
	}
	return nil
}

func sortSnapshotFileDiff(files []iceberg.DataFile) {
	slices.SortFunc(files, func(a, b iceberg.DataFile) int {
		return cmp.Compare(a.FilePath(), b.FilePath())
	})
}

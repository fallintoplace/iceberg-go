// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to You under the Apache License, Version 2.0 (the
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
	"maps"
	"slices"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// SnapshotFileDiff is the net difference in live content files between two
// snapshots. AddedFiles and RemovedFiles contain complete manifest data-file
// metadata. On v1 tables they contain only data files; on v2/v3 tables they
// may also contain positional and equality delete files.
//
// A manifest rewrite with no change to live files produces an empty diff.
// Files are ordered by path, then by content type, for stable results.
type SnapshotFileDiff struct {
	FromSnapshotID int64
	ToSnapshotID   int64
	AddedFiles     []iceberg.DataFile
	RemovedFiles   []iceberg.DataFile
}

// Empty reports whether the snapshots reference the same set of live files.
func (d SnapshotFileDiff) Empty() bool {
	return len(d.AddedFiles) == 0 && len(d.RemovedFiles) == 0
}

// DiffSnapshotFiles compares the live content files of two retained snapshots.
// The snapshots need not be ancestors of one another: this works for rollbacks,
// branches, and manifest rewrites as well as normal append/delete snapshots.
//
// Per the Iceberg v1-v3 spec, manifests are immutable and snapshots may reuse
// them. Manifests present in both snapshots are skipped without reading their
// entries. Only the remaining manifests are decoded; a file moved between
// different manifests appears in neither AddedFiles nor RemovedFiles.
//
// Deleted manifest entries are tombstones and are never counted as live files.
// This is a metadata-only operation: it does not read data files or attempt a
// row-level diff. The table must still retain both snapshot IDs.
func (t Table) DiffSnapshotFiles(ctx context.Context, fromSnapshotID, toSnapshotID int64) (SnapshotFileDiff, error) {
	diff := SnapshotFileDiff{FromSnapshotID: fromSnapshotID, ToSnapshotID: toSnapshotID}

	if ctx == nil {
		return diff, fmt.Errorf("%w: nil context", iceberg.ErrInvalidArgument)
	}
	if err := ctx.Err(); err != nil {
		return diff, err
	}
	if t.metadata == nil {
		return diff, fmt.Errorf("%w: table metadata is not configured", ErrInvalidOperation)
	}
	from := t.metadata.SnapshotByID(fromSnapshotID)
	if from == nil {
		return diff, fmt.Errorf("%w: source snapshot %d does not exist", iceberg.ErrInvalidArgument, fromSnapshotID)
	}
	to := t.metadata.SnapshotByID(toSnapshotID)
	if to == nil {
		return diff, fmt.Errorf("%w: destination snapshot %d does not exist", iceberg.ErrInvalidArgument, toSnapshotID)
	}
	if fromSnapshotID == toSnapshotID {
		return diff, nil
	}

	provider := t.ManifestProvider()
	before, err := provider.Manifests(ctx, *from)
	if err != nil {
		return diff, fmt.Errorf("load source snapshot %d manifests: %w", fromSnapshotID, err)
	}
	after, err := provider.Manifests(ctx, *to)
	if err != nil {
		return diff, fmt.Errorf("load destination snapshot %d manifests: %w", toSnapshotID, err)
	}

	beforeOnly, err := uniqueDiffManifests(before)
	if err != nil {
		return diff, fmt.Errorf("source snapshot %d: %w", fromSnapshotID, err)
	}
	afterOnly, err := uniqueDiffManifests(after)
	if err != nil {
		return diff, fmt.Errorf("destination snapshot %d: %w", toSnapshotID, err)
	}
	for path := range beforeOnly {
		if _, shared := afterOnly[path]; shared {
			delete(beforeOnly, path)
			delete(afterOnly, path)
		}
	}
	if len(beforeOnly) == 0 && len(afterOnly) == 0 {
		return diff, nil
	}

	fs, err := provider.FS(ctx)
	if err != nil {
		return diff, fmt.Errorf("load file IO for snapshot diff: %w", err)
	}

	beforeFiles, err := liveDiffFiles(ctx, fs, beforeOnly)
	if err != nil {
		return diff, fmt.Errorf("read source snapshot %d: %w", fromSnapshotID, err)
	}
	afterFiles, err := liveDiffFiles(ctx, fs, afterOnly)
	if err != nil {
		return diff, fmt.Errorf("read destination snapshot %d: %w", toSnapshotID, err)
	}

	for key, file := range beforeFiles {
		if _, stillLive := afterFiles[key]; stillLive {
			delete(afterFiles, key)
		} else {
			diff.RemovedFiles = append(diff.RemovedFiles, file)
		}
	}
	for _, file := range afterFiles {
		diff.AddedFiles = append(diff.AddedFiles, file)
	}
	sortDiffFiles(diff.AddedFiles)
	sortDiffFiles(diff.RemovedFiles)

	return diff, nil
}

func uniqueDiffManifests(seq func(func(iceberg.ManifestFile) bool)) (map[string]iceberg.ManifestFile, error) {
	manifests := make(map[string]iceberg.ManifestFile)
	var duplicate string
	seq(func(manifest iceberg.ManifestFile) bool {
		path := manifest.FilePath()
		if _, exists := manifests[path]; exists {
			duplicate = path
			return false
		}
		manifests[path] = manifest
		return true
	})
	if duplicate != "" {
		return nil, fmt.Errorf("%w: duplicate manifest %q", iceberg.ErrInvalidArgument, duplicate)
	}
	return manifests, nil
}

type diffFileKey struct {
	path    string
	content iceberg.ManifestEntryContent
}

func liveDiffFiles(ctx context.Context, fs iceio.IO, manifests map[string]iceberg.ManifestFile) (map[diffFileKey]iceberg.DataFile, error) {
	files := make(map[diffFileKey]iceberg.DataFile)
	// Iterating paths in sorted order makes corruption errors deterministic.
	for _, path := range slices.Sorted(maps.Keys(manifests)) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for entry, err := range manifests[path].Entries(fs, true) {
			if err != nil {
				return nil, fmt.Errorf("manifest %q: %w", path, err)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			file := entry.DataFile()
			key := diffFileKey{path: file.FilePath(), content: file.ContentType()}
			if _, exists := files[key]; exists {
				return nil, fmt.Errorf("%w: duplicate live content file %q in changed manifests", iceberg.ErrInvalidArgument, file.FilePath())
			}
			files[key] = file
		}
	}
	return files, nil
}

func sortDiffFiles(files []iceberg.DataFile) {
	slices.SortFunc(files, func(a, b iceberg.DataFile) int {
		if a.FilePath() < b.FilePath() {
			return -1
		}
		if a.FilePath() > b.FilePath() {
			return 1
		}
		if a.ContentType() < b.ContentType() {
			return -1
		}
		if a.ContentType() > b.ContentType() {
			return 1
		}
		return 0
	})
}

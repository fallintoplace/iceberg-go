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

// SnapshotFileDiff describes the net change in live files between two table
// snapshots. Added and removed slices are ordered by file path, then content
// type. It describes physical files, not row-level changes: a delete file
// changes the visible rows of existing data files without replacing them.
// A file still present in both snapshots is not returned, even if it was
// removed and added again in the intervening history.
type SnapshotFileDiff struct {
	FromSnapshotID     int64
	ToSnapshotID       int64
	AddedDataFiles     []iceberg.DataFile
	RemovedDataFiles   []iceberg.DataFile
	AddedDeleteFiles   []iceberg.DataFile
	RemovedDeleteFiles []iceberg.DataFile
}

type snapshotFileKey struct {
	content iceberg.ManifestEntryContent
	path    string
}

// DiffSnapshotFiles compares the live physical file sets of two snapshots.
// It supports unrelated or divergent snapshots, not just ancestor chains.
// Manifests referenced by both snapshots are immutable and can be skipped
// entirely. The remaining manifest entries are streamed and only their live
// files are compared. A missing snapshot returns ErrSnapshotNotFound.
//
// The result reflects the table metadata used to construct this Table. Call
// Refresh or reload the table to include later commits. Neither the table nor
// its snapshots are modified.
func (t Table) DiffSnapshotFiles(ctx context.Context, fromSnapshotID, toSnapshotID int64) (SnapshotFileDiff, error) {
	diff := SnapshotFileDiff{FromSnapshotID: fromSnapshotID, ToSnapshotID: toSnapshotID}
	if err := ctx.Err(); err != nil {
		return diff, err
	}
	if t.metadata == nil {
		return diff, fmt.Errorf("%w: table metadata is not configured", ErrInvalidOperation)
	}

	from := t.SnapshotByID(fromSnapshotID)
	if from == nil {
		return diff, fmt.Errorf("%w: source snapshot %d", ErrSnapshotNotFound, fromSnapshotID)
	}
	to := t.SnapshotByID(toSnapshotID)
	if to == nil {
		return diff, fmt.Errorf("%w: target snapshot %d", ErrSnapshotNotFound, toSnapshotID)
	}
	if fromSnapshotID == toSnapshotID {
		return diff, nil
	}

	provider := t.ManifestProvider()
	fromIter, err := provider.Manifests(ctx, *from)
	if err != nil {
		return diff, fmt.Errorf("source snapshot %d manifests: %w", fromSnapshotID, err)
	}
	toIter, err := provider.Manifests(ctx, *to)
	if err != nil {
		return diff, fmt.Errorf("target snapshot %d manifests: %w", toSnapshotID, err)
	}
	oldManifests, newManifests := snapshotDiffUniqueManifests(slices.Collect(fromIter), slices.Collect(toIter))
	if len(oldManifests) == 0 && len(newManifests) == 0 {
		return diff, ctx.Err()
	}

	fs, err := provider.FS(ctx)
	if err != nil {
		return diff, fmt.Errorf("snapshot file diff IO: %w", err)
	}
	oldFiles, err := readSnapshotDiffFiles(ctx, fs, oldManifests)
	if err != nil {
		return diff, fmt.Errorf("source snapshot %d files: %w", fromSnapshotID, err)
	}
	newFiles, err := readSnapshotDiffFiles(ctx, fs, newManifests)
	if err != nil {
		return diff, fmt.Errorf("target snapshot %d files: %w", toSnapshotID, err)
	}

	for key, file := range oldFiles {
		if _, present := newFiles[key]; present {
			continue
		}
		if err := diff.appendFile(file, false); err != nil {
			return SnapshotFileDiff{}, err
		}
	}
	for key, file := range newFiles {
		if _, present := oldFiles[key]; present {
			continue
		}
		if err := diff.appendFile(file, true); err != nil {
			return SnapshotFileDiff{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return SnapshotFileDiff{}, err
	}
	diff.sortFiles()
	return diff, nil
}

// DiffSnapshotRefs compares the snapshots currently selected by two named
// refs, including tags or divergent branches. Both refs must exist and point
// at snapshots retained in table metadata.
func (t Table) DiffSnapshotRefs(ctx context.Context, fromRef, toRef string) (SnapshotFileDiff, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotFileDiff{}, err
	}
	if t.metadata == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: table metadata is not configured", ErrInvalidOperation)
	}
	from := t.SnapshotByName(fromRef)
	if from == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: source ref %q", ErrSnapshotNotFound, fromRef)
	}
	to := t.SnapshotByName(toRef)
	if to == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: target ref %q", ErrSnapshotNotFound, toRef)
	}
	return t.DiffSnapshotFiles(ctx, from.SnapshotID, to.SnapshotID)
}

// snapshotDiffUniqueManifests removes manifests shared between both snapshots.
// Manifest locations are immutable and uniquely identify their contents, so
// skipping a shared manifest is safe even across divergent branches.
func snapshotDiffUniqueManifests(from, to []iceberg.ManifestFile) ([]iceberg.ManifestFile, []iceberg.ManifestFile) {
	oldPaths := make(map[string]struct{}, len(from))
	newPaths := make(map[string]struct{}, len(to))
	for _, manifest := range from {
		oldPaths[manifest.FilePath()] = struct{}{}
	}
	for _, manifest := range to {
		newPaths[manifest.FilePath()] = struct{}{}
	}
	oldOnly := make([]iceberg.ManifestFile, 0, len(from))
	newOnly := make([]iceberg.ManifestFile, 0, len(to))
	seen := make(map[string]struct{})
	for _, manifest := range from {
		path := manifest.FilePath()
		if _, ok := newPaths[path]; ok {
			continue
		}
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		oldOnly = append(oldOnly, manifest)
	}
	clear(seen)
	for _, manifest := range to {
		path := manifest.FilePath()
		if _, ok := oldPaths[path]; ok {
			continue
		}
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		newOnly = append(newOnly, manifest)
	}
	return oldOnly, newOnly
}

func readSnapshotDiffFiles(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) (map[snapshotFileKey]iceberg.DataFile, error) {
	files := make(map[snapshotFileKey]iceberg.DataFile)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, fmt.Errorf("manifest %q: %w", manifest.FilePath(), err)
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if entry == nil || entry.DataFile() == nil {
				return nil, fmt.Errorf("manifest %q: nil live file", manifest.FilePath())
			}
			file := entry.DataFile()
			key := snapshotFileKey{content: file.ContentType(), path: file.FilePath()}
			files[key] = file
		}
	}
	return files, nil
}

func (d *SnapshotFileDiff) appendFile(file iceberg.DataFile, added bool) error {
	switch file.ContentType() {
	case iceberg.EntryContentData:
		if added {
			d.AddedDataFiles = append(d.AddedDataFiles, file)
		} else {
			d.RemovedDataFiles = append(d.RemovedDataFiles, file)
		}
	case iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
		if added {
			d.AddedDeleteFiles = append(d.AddedDeleteFiles, file)
		} else {
			d.RemovedDeleteFiles = append(d.RemovedDeleteFiles, file)
		}
	default:
		return fmt.Errorf("unsupported file content %d at %q", file.ContentType(), file.FilePath())
	}
	return nil
}

func (d *SnapshotFileDiff) sortFiles() {
	less := func(a, b iceberg.DataFile) int {
		if order := cmp.Compare(a.FilePath(), b.FilePath()); order != 0 {
			return order
		}
		return cmp.Compare(a.ContentType(), b.ContentType())
	}
	slices.SortFunc(d.AddedDataFiles, less)
	slices.SortFunc(d.RemovedDataFiles, less)
	slices.SortFunc(d.AddedDeleteFiles, less)
	slices.SortFunc(d.RemovedDeleteFiles, less)
}

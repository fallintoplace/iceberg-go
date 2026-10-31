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

// SnapshotFileDiff describes the net change in live content files between two
// snapshots. Files that exist in both snapshots are omitted, even when a
// manifest rewrite moves their entries to different manifests.
//
// The comparison is independent of snapshot ancestry and can compare two
// branches or tags. Delete files include position deletes, equality deletes,
// and v3 deletion vectors. This is a file-level diff, not a row-level changelog:
// removing or adding a delete file does not necessarily change the data files.
type SnapshotFileDiff struct {
	FromSnapshotID int64
	ToSnapshotID   int64

	AddedDataFiles     []iceberg.DataFile
	RemovedDataFiles   []iceberg.DataFile
	AddedDeleteFiles   []iceberg.DataFile
	RemovedDeleteFiles []iceberg.DataFile
}

// DiffSnapshotFilesByRef compares the snapshots pointed to by two named refs.
// Both refs must be present in the table metadata. Missing refs or refs whose
// snapshots have expired return ErrSnapshotNotFound.
func (t Table) DiffSnapshotFilesByRef(ctx context.Context, fromRef, toRef string) (SnapshotFileDiff, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotFileDiff{}, err
	}
	if t.metadata == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: table metadata is not available", ErrInvalidMetadata)
	}
	from := t.SnapshotByName(fromRef)
	if from == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: ref %q", ErrSnapshotNotFound, fromRef)
	}
	to := t.SnapshotByName(toRef)
	if to == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: ref %q", ErrSnapshotNotFound, toRef)
	}

	return t.DiffSnapshotFiles(ctx, from.SnapshotID, to.SnapshotID)
}

// DiffSnapshotFiles compares the live data and delete files of two retained
// snapshots without reading data rows. Both IDs must exist in table metadata.
//
// Shared immutable manifests are skipped entirely. Files carried over by
// different manifests are compared by their content-file identity, not by
// manifest-entry status or snapshot ID. In particular, v3 deletion vectors
// in the same Puffin file are distinguished by referenced data file and blob
// offset and length. Results are sorted by file identity for stable output.
//
// The operation uses memory proportional to the live files in the manifests
// that differ between the two snapshots, rather than all the table's files.
// On errors no partial diff is returned.
func (t Table) DiffSnapshotFiles(ctx context.Context, fromSnapshotID, toSnapshotID int64) (SnapshotFileDiff, error) {
	if err := ctx.Err(); err != nil {
		return SnapshotFileDiff{}, err
	}
	if t.metadata == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: table metadata is not available", ErrInvalidMetadata)
	}

	from := t.SnapshotByID(fromSnapshotID)
	if from == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: from snapshot %d", ErrSnapshotNotFound, fromSnapshotID)
	}
	to := t.SnapshotByID(toSnapshotID)
	if to == nil {
		return SnapshotFileDiff{}, fmt.Errorf("%w: to snapshot %d", ErrSnapshotNotFound, toSnapshotID)
	}

	result := SnapshotFileDiff{FromSnapshotID: fromSnapshotID, ToSnapshotID: toSnapshotID}
	if fromSnapshotID == toSnapshotID {
		return result, nil
	}

	provider := t.ManifestProvider()
	before, err := provider.set(ctx, *from)
	if err != nil {
		return SnapshotFileDiff{}, fmt.Errorf("read manifests for snapshot %d: %w", fromSnapshotID, err)
	}
	after, err := provider.set(ctx, *to)
	if err != nil {
		return SnapshotFileDiff{}, fmt.Errorf("read manifests for snapshot %d: %w", toSnapshotID, err)
	}

	beforeOnly, afterOnly, err := differingSnapshotManifests(before.borrowAllManifests(), after.borrowAllManifests())
	if err != nil {
		return SnapshotFileDiff{}, err
	}
	if err := ctx.Err(); err != nil {
		return SnapshotFileDiff{}, err
	}
	if len(beforeOnly) == 0 && len(afterOnly) == 0 {
		return result, nil
	}

	fs, err := provider.FS(ctx)
	if err != nil {
		return SnapshotFileDiff{}, fmt.Errorf("resolve file IO for snapshot diff: %w", err)
	}
	oldFiles, err := liveSnapshotFiles(ctx, fs, beforeOnly)
	if err != nil {
		return SnapshotFileDiff{}, fmt.Errorf("read files for snapshot %d: %w", fromSnapshotID, err)
	}
	newFiles, err := liveSnapshotFiles(ctx, fs, afterOnly)
	if err != nil {
		return SnapshotFileDiff{}, fmt.Errorf("read files for snapshot %d: %w", toSnapshotID, err)
	}

	for key, file := range oldFiles {
		if _, ok := newFiles[key]; ok {
			continue
		}
		if key.content == iceberg.EntryContentData {
			result.RemovedDataFiles = append(result.RemovedDataFiles, file)
		} else {
			result.RemovedDeleteFiles = append(result.RemovedDeleteFiles, file)
		}
	}
	for key, file := range newFiles {
		if _, ok := oldFiles[key]; ok {
			continue
		}
		if key.content == iceberg.EntryContentData {
			result.AddedDataFiles = append(result.AddedDataFiles, file)
		} else {
			result.AddedDeleteFiles = append(result.AddedDeleteFiles, file)
		}
	}

	sortSnapshotDiffFiles(result.AddedDataFiles)
	sortSnapshotDiffFiles(result.RemovedDataFiles)
	sortSnapshotDiffFiles(result.AddedDeleteFiles)
	sortSnapshotDiffFiles(result.RemovedDeleteFiles)
	if err := ctx.Err(); err != nil {
		return SnapshotFileDiff{}, err
	}

	return result, nil
}

// An Iceberg manifest is immutable. Its path is sufficient to determine that
// all live entries carried by that manifest are unchanged between snapshots.
// Rewritten manifests cannot be skipped: they may carry unchanged files that
// the file-level comparison must cancel out instead.
func differingSnapshotManifests(before, after []iceberg.ManifestFile) ([]iceberg.ManifestFile, []iceberg.ManifestFile, error) {
	oldPaths := make(map[string]iceberg.ManifestFile, len(before))
	newPaths := make(map[string]iceberg.ManifestFile, len(after))
	for _, m := range before {
		if err := addSnapshotDiffManifest(oldPaths, m); err != nil {
			return nil, nil, err
		}
	}
	for _, m := range after {
		if err := addSnapshotDiffManifest(newPaths, m); err != nil {
			return nil, nil, err
		}
	}

	var oldOnly, newOnly []iceberg.ManifestFile
	for _, m := range before {
		other, ok := newPaths[m.FilePath()]
		if !ok {
			oldOnly = append(oldOnly, m)
			continue
		}
		if m.Length() != other.Length() || m.ManifestContent() != other.ManifestContent() {
			return nil, nil, fmt.Errorf("%w: shared manifest %q has inconsistent metadata", ErrInvalidMetadata, m.FilePath())
		}
	}
	for _, m := range after {
		if _, ok := oldPaths[m.FilePath()]; !ok {
			newOnly = append(newOnly, m)
		}
	}

	return oldOnly, newOnly, nil
}

func addSnapshotDiffManifest(paths map[string]iceberg.ManifestFile, m iceberg.ManifestFile) error {
	if m == nil || m.FilePath() == "" {
		return fmt.Errorf("%w: snapshot contains a manifest with no path", ErrInvalidMetadata)
	}
	if _, ok := paths[m.FilePath()]; ok {
		return fmt.Errorf("%w: duplicate manifest %q in a snapshot", ErrInvalidMetadata, m.FilePath())
	}
	paths[m.FilePath()] = m

	return nil
}

type snapshotDiffFileKey struct {
	path       string
	content    iceberg.ManifestEntryContent
	format     iceberg.FileFormat
	referenced string
	offset     int64
	length     int64
}

func snapshotDiffKey(file iceberg.DataFile) (snapshotDiffFileKey, error) {
	if file == nil || file.FilePath() == "" {
		return snapshotDiffFileKey{}, fmt.Errorf("%w: manifest contains a file with no path", ErrInvalidMetadata)
	}
	key := snapshotDiffFileKey{path: file.FilePath(), content: file.ContentType(), format: file.FileFormat()}
	switch key.content {
	case iceberg.EntryContentData, iceberg.EntryContentPosDeletes, iceberg.EntryContentEqDeletes:
	default:
		return snapshotDiffFileKey{}, fmt.Errorf("%w: unknown content type %d for %q", ErrInvalidMetadata, key.content, key.path)
	}
	if IsDeletionVector(file) {
		ref, offset, length := file.ReferencedDataFile(), file.ContentOffset(), file.ContentSizeInBytes()
		if ref == nil || *ref == "" || offset == nil || *offset < 0 || length == nil || *length <= 0 {
			return snapshotDiffFileKey{}, fmt.Errorf("%w: deletion vector %q has invalid reference, offset, or length", ErrInvalidMetadata, key.path)
		}
		key.referenced, key.offset, key.length = *ref, *offset, *length
	}

	return key, nil
}

func liveSnapshotFiles(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) (map[snapshotDiffFileKey]iceberg.DataFile, error) {
	files := make(map[snapshotDiffFileKey]iceberg.DataFile)
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
			if entry == nil {
				return nil, fmt.Errorf("%w: manifest %q contains a nil entry", ErrInvalidMetadata, manifest.FilePath())
			}
			file := entry.DataFile()
			key, err := snapshotDiffKey(file)
			if err != nil {
				return nil, fmt.Errorf("manifest %q: %w", manifest.FilePath(), err)
			}
			if manifest.ManifestContent() == iceberg.ManifestContentData && key.content != iceberg.EntryContentData ||
				manifest.ManifestContent() == iceberg.ManifestContentDeletes && key.content == iceberg.EntryContentData {
				return nil, fmt.Errorf("%w: manifest %q contains file %q with incompatible content type", ErrInvalidMetadata, manifest.FilePath(), key.path)
			}
			if _, exists := files[key]; exists {
				return nil, fmt.Errorf("%w: duplicate live content file %q in a snapshot", ErrInvalidMetadata, key.path)
			}
			files[key] = file
		}
	}

	return files, ctx.Err()
}

func sortSnapshotDiffFiles(files []iceberg.DataFile) {
	slices.SortFunc(files, func(a, b iceberg.DataFile) int {
		ak, _ := snapshotDiffKey(a) // Already validated while reading manifests.
		bk, _ := snapshotDiffKey(b)
		if n := cmp.Compare(ak.path, bk.path); n != 0 {
			return n
		}
		if n := cmp.Compare(ak.content, bk.content); n != 0 {
			return n
		}
		if n := cmp.Compare(ak.format, bk.format); n != 0 {
			return n
		}
		if n := cmp.Compare(ak.referenced, bk.referenced); n != 0 {
			return n
		}
		if n := cmp.Compare(ak.offset, bk.offset); n != 0 {
			return n
		}

		return cmp.Compare(ak.length, bk.length)
	})
}

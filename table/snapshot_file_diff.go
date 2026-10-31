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
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// SnapshotFileChange describes a live file present on one side of a snapshot
// comparison. File can be a data, positional-delete, or equality-delete file.
// The sequence numbers and snapshot ID come from the manifest entry that
// describes the file, not from the snapshot being compared. File and its
// metadata must be treated as read-only.
type SnapshotFileChange struct {
	File               iceberg.DataFile
	PartitionSpecID    int32
	AddedSnapshotID    int64
	DataSequenceNumber int64
	FileSequenceNumber *int64
}

// SnapshotFileDiff is the net difference between the *live* file inventories
// of two snapshots. A file that is present at both endpoints is not reported,
// even when its manifest was rewritten between them. Added and Removed are
// ordered by path, then file content type, for reproducible output.
//
// This compares file identity, not rows. In particular, a delete file being
// added does not mean its affected data rows are included in Removed.
type SnapshotFileDiff struct {
	FromSnapshotID int64
	ToSnapshotID   int64
	Added          []SnapshotFileChange
	Removed        []SnapshotFileChange
}

type snapshotFileIdentity struct {
	path    string
	content iceberg.ManifestEntryContent
}

// DiffSnapshotFiles returns the files introduced and removed between two
// snapshots, including data and delete files. Both snapshot IDs must exist in
// this table's metadata. They do not need to be in the same ancestry chain:
// comparing the heads of two branches reports their net file inventory
// difference. Passing the same snapshot twice produces an empty diff.
//
// Shared manifest paths are immutable and need not be opened. Remaining live
// entries are compared by (content type, file path), so manifest rewrites and
// status changes from ADDED to EXISTING do not produce spurious changes.
// The implementation holds only unique-manifest entries in memory, rather than
// collecting the entire inventories of both snapshots. It respects ctx before
// reads and between manifest entries; the underlying reader can still finish
// an in-progress file read when the FileIO does not support cancellation.
func (t Table) DiffSnapshotFiles(ctx context.Context, fromSnapshotID, toSnapshotID int64) (SnapshotFileDiff, error) {
	diff := SnapshotFileDiff{FromSnapshotID: fromSnapshotID, ToSnapshotID: toSnapshotID}
	if err := ctx.Err(); err != nil {
		return diff, err
	}
	if t.metadata == nil {
		return diff, fmt.Errorf("%w: table metadata is not available", iceberg.ErrInvalidArgument)
	}
	from := t.metadata.SnapshotByID(fromSnapshotID)
	if from == nil {
		return diff, fmt.Errorf("%w: starting snapshot %d not found", iceberg.ErrInvalidArgument, fromSnapshotID)
	}
	to := t.metadata.SnapshotByID(toSnapshotID)
	if to == nil {
		return diff, fmt.Errorf("%w: ending snapshot %d not found", iceberg.ErrInvalidArgument, toSnapshotID)
	}
	if fromSnapshotID == toSnapshotID || sameSnapshotManifestSources(*from, *to) {
		return diff, nil
	}

	provider := t.ManifestProvider()
	fromSeq, err := provider.Manifests(ctx, *from)
	if err != nil {
		return diff, fmt.Errorf("load manifests for snapshot %d: %w", fromSnapshotID, err)
	}
	toSeq, err := provider.Manifests(ctx, *to)
	if err != nil {
		return diff, fmt.Errorf("load manifests for snapshot %d: %w", toSnapshotID, err)
	}
	fromManifests, err := indexSnapshotManifestPaths(ctx, fromSeq)
	if err != nil {
		return diff, fmt.Errorf("snapshot %d: %w", fromSnapshotID, err)
	}
	toManifests, err := indexSnapshotManifestPaths(ctx, toSeq)
	if err != nil {
		return diff, fmt.Errorf("snapshot %d: %w", toSnapshotID, err)
	}

	// Avoid obtaining FileIO if every manifest was reused. In that case the
	// expensive entry scan is unnecessary, even if manifest lists differ.
	changedFrom := nonSharedSnapshotManifests(fromManifests, toManifests)
	changedTo := nonSharedSnapshotManifests(toManifests, fromManifests)
	if len(changedFrom) == 0 && len(changedTo) == 0 {
		return diff, nil
	}

	fs, err := provider.FS(ctx)
	if err != nil {
		return diff, fmt.Errorf("get file IO: %w", err)
	}
	// A missing backend must be reported instead of invoking Entries on nil.
	if fs == nil {
		return diff, fmt.Errorf("%w: table file IO is not configured", ErrInvalidOperation)
	}

	fromFiles, err := collectSnapshotDiffFiles(ctx, fs, changedFrom)
	if err != nil {
		return diff, fmt.Errorf("read snapshot %d files: %w", fromSnapshotID, err)
	}
	toFiles, err := collectSnapshotDiffFiles(ctx, fs, changedTo)
	if err != nil {
		return diff, fmt.Errorf("read snapshot %d files: %w", toSnapshotID, err)
	}

	for identity, entry := range toFiles {
		if _, existed := fromFiles[identity]; !existed {
			diff.Added = append(diff.Added, entry)
		}
	}
	for identity, entry := range fromFiles {
		if _, exists := toFiles[identity]; !exists {
			diff.Removed = append(diff.Removed, entry)
		}
	}
	sortSnapshotFileChanges(diff.Added)
	sortSnapshotFileChanges(diff.Removed)

	return diff, nil
}

func sameSnapshotManifestSources(left, right Snapshot) bool {
	if left.ManifestList != "" || right.ManifestList != "" {
		return left.ManifestList != "" && left.ManifestList == right.ManifestList
	}

	return (left.ManifestLocations == nil) == (right.ManifestLocations == nil) &&
		slices.Equal(left.ManifestLocations, right.ManifestLocations)
}

func indexSnapshotManifestPaths(ctx context.Context, manifests func(func(iceberg.ManifestFile) bool)) (map[string]iceberg.ManifestFile, error) {
	byPath := make(map[string]iceberg.ManifestFile)
	var readErr error
	manifests(func(m iceberg.ManifestFile) bool {
		if err := ctx.Err(); err != nil {
			readErr = err
			return false
		}
		path := m.FilePath()
		if path == "" {
			readErr = fmt.Errorf("%w: empty manifest path", ErrInvalidMetadata)
			return false
		}
		if _, duplicate := byPath[path]; duplicate {
			readErr = fmt.Errorf("%w: duplicate manifest path %q", ErrInvalidMetadata, path)
			return false
		}
		byPath[path] = m
		return true
	})

	return byPath, readErr
}

func nonSharedSnapshotManifests(source, other map[string]iceberg.ManifestFile) []iceberg.ManifestFile {
	result := make([]iceberg.ManifestFile, 0, len(source))
	for path, manifest := range source {
		if _, shared := other[path]; !shared {
			result = append(result, manifest)
		}
	}
	slices.SortFunc(result, func(a, b iceberg.ManifestFile) int {
		return cmp.Compare(a.FilePath(), b.FilePath())
	})

	return result
}

func collectSnapshotDiffFiles(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) (map[snapshotFileIdentity]SnapshotFileChange, error) {
	files := make(map[snapshotFileIdentity]SnapshotFileChange)
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
				return nil, fmt.Errorf("%w: manifest %q contains a nil file entry", ErrInvalidMetadata, manifest.FilePath())
			}
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			file := entry.DataFile()
			identity := snapshotFileIdentity{path: file.FilePath(), content: file.ContentType()}
			if identity.path == "" {
				return nil, fmt.Errorf("%w: manifest %q contains an empty data file path", ErrInvalidMetadata, manifest.FilePath())
			}
			if _, duplicated := files[identity]; duplicated {
				return nil, fmt.Errorf("%w: duplicate live file %q (content %d)", ErrInvalidMetadata, identity.path, identity.content)
			}
			files[identity] = SnapshotFileChange{
				File:               file,
				PartitionSpecID:    manifest.PartitionSpecID(),
				AddedSnapshotID:    entry.SnapshotID(),
				DataSequenceNumber: entry.DataSequenceNumber(),
				FileSequenceNumber: entry.FileSequenceNumber(),
			}
		}
	}

	return files, nil
}

func sortSnapshotFileChanges(changes []SnapshotFileChange) {
	slices.SortFunc(changes, func(left, right SnapshotFileChange) int {
		if byPath := cmp.Compare(left.File.FilePath(), right.File.FilePath()); byPath != 0 {
			return byPath
		}
		return cmp.Compare(left.File.ContentType(), right.File.ContentType())
	})
}

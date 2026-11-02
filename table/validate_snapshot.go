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

	"github.com/apache/iceberg-go"
)

// SnapshotValidationIssueCode describes a violation of the Iceberg snapshot
// manifest and content-file invariants.
type SnapshotValidationIssueCode string

const (
	SnapshotIssueDuplicateManifest           SnapshotValidationIssueCode = "duplicate-manifest"
	SnapshotIssueDuplicateLiveFile           SnapshotValidationIssueCode = "duplicate-live-file"
	SnapshotIssueUnknownPartitionSpec        SnapshotValidationIssueCode = "unknown-partition-spec"
	SnapshotIssueIncompatibleManifestVersion SnapshotValidationIssueCode = "incompatible-manifest-version"
	SnapshotIssueInvalidManifestContent      SnapshotValidationIssueCode = "invalid-manifest-content"
	SnapshotIssueInvalidFileContent          SnapshotValidationIssueCode = "invalid-file-content"
	SnapshotIssueInvalidFileFormat           SnapshotValidationIssueCode = "invalid-file-format"
	SnapshotIssueEmptyManifestPath           SnapshotValidationIssueCode = "empty-manifest-path"
	SnapshotIssueEmptyFilePath               SnapshotValidationIssueCode = "empty-file-path"
	SnapshotIssueManifestCountMismatch       SnapshotValidationIssueCode = "manifest-count-mismatch"
)

// SnapshotValidationIssue identifies a manifest-level or live-file-level
// problem. FirstManifestPath is populated for duplicate paths. Detail gives
// additional context such as the mismatched manifest count.
type SnapshotValidationIssue struct {
	Code              SnapshotValidationIssueCode
	ManifestPath      string
	FilePath          string
	FirstManifestPath string
	PartitionSpecID   int32
	Detail            string
}

// SnapshotValidationReport describes the metadata inspected for one snapshot.
// The audit does not read data-file contents or verify their storage existence.
type SnapshotValidationReport struct {
	SnapshotID       int64
	ManifestsChecked int64
	LiveFilesChecked int64
	Issues           []SnapshotValidationIssue
	Complete         bool
}

// Valid is true only after the entire audit completes without issues.
func (r SnapshotValidationReport) Valid() bool {
	return r.Complete && len(r.Issues) == 0
}

// ValidateSnapshot audits the manifest list and manifest entries of a snapshot.
//
// It checks that the manifest list references known partition specs, that
// manifests and file contents are valid for the table's format version, and
// that a content file has at most one live reference across all manifests.
// Deleted entries do not count as live references. Manifest entry counts are
// checked only when the manifest list provides them (v1 may omit them).
//
// The snapshot is never modified. I/O errors and cancellation return the
// partial report with Complete=false and a non-nil error. The audit reads
// manifest metadata but does not read or stat data files.
//
// Spec: https://iceberg.apache.org/spec/#content-file-uniqueness
// Spec: https://iceberg.apache.org/spec/#manifest-lists
func (t Table) ValidateSnapshot(ctx context.Context, snapshotID int64) (*SnapshotValidationReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is nil", iceberg.ErrInvalidArgument)
	}
	if t.metadata == nil {
		return nil, fmt.Errorf("%w: table metadata is nil", iceberg.ErrInvalidArgument)
	}

	snapshot := t.metadata.SnapshotByID(snapshotID)
	if snapshot == nil {
		return nil, fmt.Errorf("%w: snapshot %d not found", iceberg.ErrInvalidArgument, snapshotID)
	}
	report := &SnapshotValidationReport{SnapshotID: snapshotID}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	fs, err := t.FS(ctx)
	if err != nil {
		return report, fmt.Errorf("load snapshot %d FileIO: %w", snapshotID, err)
	}
	if fs == nil {
		return report, fmt.Errorf("snapshot %d FileIO is nil", snapshotID)
	}

	manifests, err := snapshot.Manifests(fs)
	if err != nil {
		return report, fmt.Errorf("read snapshot %d manifest list: %w", snapshotID, err)
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	specIDs := make(map[int]struct{})
	for _, spec := range t.metadata.PartitionSpecs() {
		specIDs[spec.ID()] = struct{}{}
	}
	version := t.metadata.FormatVersion()
	seenManifests := make(map[string]struct{}, len(manifests))
	// The map intentionally tracks every live content file, regardless of its
	// partition spec, entry status (ADDED/EXISTING), or file format.
	seenFiles := make(map[string]string)

	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.ManifestsChecked++
		if manifest == nil {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueEmptyManifestPath, Detail: "manifest entry is nil",
			})
			continue
		}
		path := manifest.FilePath()
		if path == "" {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueEmptyManifestPath,
			})
			continue
		}
		if _, exists := seenManifests[path]; exists {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueDuplicateManifest, ManifestPath: path,
			})
		}
		seenManifests[path] = struct{}{}

		validateSnapshotManifestDescriptor(report, manifest, version, specIDs)

		var added, existing, deleted int64
		for entry, entryErr := range manifest.Entries(fs, false) {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			if entryErr != nil {
				return report, fmt.Errorf("read snapshot %d manifest %q: %w", snapshotID, path, entryErr)
			}
			if entry == nil {
				return report, fmt.Errorf("read snapshot %d manifest %q: nil entry", snapshotID, path)
			}

			switch entry.Status() {
			case iceberg.EntryStatusDELETED:
				deleted++
				continue
			case iceberg.EntryStatusADDED:
				added++
			case iceberg.EntryStatusEXISTING:
				existing++
			default:
				return report, fmt.Errorf("read snapshot %d manifest %q: unexpected entry status %d",
					snapshotID, path, entry.Status())
			}

			report.LiveFilesChecked++
			file := entry.DataFile()
			if file == nil {
				return report, fmt.Errorf("read snapshot %d manifest %q: nil data file", snapshotID, path)
			}
			filePath := file.FilePath()
			if filePath == "" {
				report.Issues = append(report.Issues, SnapshotValidationIssue{
					Code: SnapshotIssueEmptyFilePath, ManifestPath: path,
				})
				continue
			}
			validateSnapshotFile(report, version, manifest, file)
			if firstManifest, exists := seenFiles[filePath]; exists {
				report.Issues = append(report.Issues, SnapshotValidationIssue{
					Code: SnapshotIssueDuplicateLiveFile, ManifestPath: path,
					FirstManifestPath: firstManifest, FilePath: filePath,
				})
			} else {
				seenFiles[filePath] = path
			}
		}
		validateManifestEntryCount(report, path, "added", manifest.AddedDataFiles(), added)
		validateManifestEntryCount(report, path, "existing", manifest.ExistingDataFiles(), existing)
		validateManifestEntryCount(report, path, "deleted", manifest.DeletedDataFiles(), deleted)
	}

	report.Complete = true
	return report, nil
}

func validateSnapshotManifestDescriptor(
	report *SnapshotValidationReport,
	manifest iceberg.ManifestFile,
	version int,
	specIDs map[int]struct{},
) {
	path := manifest.FilePath()
	if _, found := specIDs[int(manifest.PartitionSpecID())]; !found {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueUnknownPartitionSpec, ManifestPath: path,
			PartitionSpecID: manifest.PartitionSpecID(),
		})
	}
	if manifest.Version() < 1 || manifest.Version() > version {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueIncompatibleManifestVersion, ManifestPath: path,
			Detail: fmt.Sprintf("manifest v%d in table v%d", manifest.Version(), version),
		})
	}
	content := manifest.ManifestContent()
	if content != iceberg.ManifestContentData &&
		(version == 1 || content != iceberg.ManifestContentDeletes) {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueInvalidManifestContent, ManifestPath: path,
			Detail: fmt.Sprintf("manifest content %s in table v%d", content, version),
		})
	}
}

func validateSnapshotFile(
	report *SnapshotValidationReport,
	version int,
	manifest iceberg.ManifestFile,
	file iceberg.DataFile,
) {
	path := manifest.FilePath()
	content := file.ContentType()
	manifestContent := manifest.ManifestContent()
	if (manifestContent == iceberg.ManifestContentData && content != iceberg.EntryContentData) ||
		(manifestContent == iceberg.ManifestContentDeletes &&
			content != iceberg.EntryContentPosDeletes && content != iceberg.EntryContentEqDeletes) ||
		(version == 1 && content != iceberg.EntryContentData) {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueInvalidFileContent, ManifestPath: path,
			FilePath: file.FilePath(),
			Detail: fmt.Sprintf("file content %s in manifest %s", content, manifestContent),
		})
	}

	switch file.FileFormat() {
	case iceberg.AvroFile, iceberg.OrcFile, iceberg.ParquetFile:
	case iceberg.PuffinFile:
		if version < 3 || content != iceberg.EntryContentPosDeletes {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueInvalidFileFormat, ManifestPath: path, FilePath: file.FilePath(),
				Detail: "Puffin is only valid for positional deletes in v3",
			})
		}
	default:
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueInvalidFileFormat, ManifestPath: path, FilePath: file.FilePath(),
			Detail: fmt.Sprintf("unknown file format %q", file.FileFormat()),
		})
	}
}

func validateManifestEntryCount(
	report *SnapshotValidationReport, manifestPath, kind string, expected int32, actual int64,
) {
	// V1 counts are optional. Decoders represent null by -1.
	if expected < 0 || int64(expected) == actual {
		return
	}
	report.Issues = append(report.Issues, SnapshotValidationIssue{
		Code: SnapshotIssueManifestCountMismatch, ManifestPath: manifestPath,
		Detail: fmt.Sprintf("%s entries: manifest list reports %d, found %d", kind, expected, actual),
	})
}

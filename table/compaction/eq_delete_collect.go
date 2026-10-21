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

package compaction

import (
	"context"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// CollectDeadEqualityDeletes walks the given snapshot's manifests and returns
// equality delete files that are provably dead after a rewrite removes the
// data files in rewrittenPaths. It preserves the original conservative API,
// which treats every candidate as potentially global because it has no spec
// metadata. New callers should use [CollectDeadEqualityDeletesWithSpecs].
//
// "Dead" means: no surviving data file in the snapshot (excluding
// rewrittenPaths) could apply to the eq-delete under the conservative
// compatibility predicate (see [DecideDeadEqualityDeletes]).
//
// rewrittenPaths is the union of every old data file path being
// replaced by a planned rewrite (across all groups). The caller
// typically computes this from a compaction plan.
//
// The returned files are safe to remove during the same commit that
// stages the rewrite — they cannot be resurrected by any concurrent
// commit because (1) any concurrent eq-delete is rejected by the
// rewrite-validator, and (2) any concurrent data file gets a fresh
// sequence number strictly greater than every preexisting eq-delete,
// so it cannot satisfy E.seq > D.seq for any preexisting E.
//
// Two-pass design: the first pass walks delete manifests only to
// discover candidate eq-deletes; if there are none, the more
// expensive data-manifest walk is skipped.
func CollectDeadEqualityDeletes(
	ctx context.Context,
	fs iceio.IO,
	snap *table.Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	return collectDeadEqualityDeletes(ctx, fs, nil, snap, rewrittenPaths)
}

// CollectDeadEqualityDeletesWithSpecs walks the given snapshot's manifests and
// uses partition spec metadata to identify dead equality deletes exactly.
func CollectDeadEqualityDeletesWithSpecs(
	ctx context.Context,
	fs iceio.IO,
	specs PartitionSpecLookup,
	snap *table.Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	return collectDeadEqualityDeletes(ctx, fs, specs, snap, rewrittenPaths)
}

func collectDeadEqualityDeletes(
	ctx context.Context,
	fs iceio.IO,
	specs PartitionSpecLookup,
	snap *table.Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	if snap == nil || len(rewrittenPaths) == 0 {
		return nil, nil
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	// Split by content once and keep snapshot order within each kind.
	var deleteManifests, dataManifests []iceberg.ManifestFile
	for _, m := range manifests {
		switch m.ManifestContent() {
		case iceberg.ManifestContentDeletes:
			deleteManifests = append(deleteManifests, m)
		case iceberg.ManifestContentData:
			dataManifests = append(dataManifests, m)
		}
	}

	// Phase 1: read only delete manifests. Workers own separate slices, and
	// merging by manifest index preserves the original candidate order.
	candidateGroups := make([][]iceberg.ManifestEntry, len(deleteManifests))
	err = walkCompactionManifests(ctx, deleteManifests,
		func(readCtx context.Context, _ int, index int, m iceberg.ManifestFile) error {
			var candidates []iceberg.ManifestEntry
			for entry, readErr := range m.Entries(fs, true) {
				if readErr != nil {
					return readErr
				}
				if err := readCtx.Err(); err != nil {
					return err
				}
				if entry.DataFile().ContentType() == iceberg.EntryContentEqDeletes {
					candidates = append(candidates, entry)
				}
			}
			candidateGroups[index] = candidates

			return nil
		})
	if err != nil {
		return nil, err
	}

	candidateCount := 0
	for _, group := range candidateGroups {
		candidateCount += len(group)
	}
	if candidateCount == 0 {
		return nil, nil
	}
	candidates := make([]iceberg.ManifestEntry, 0, candidateCount)
	for _, group := range candidateGroups {
		candidates = append(candidates, group...)
	}

	// Phase 2: each worker maintains its own survivor minima. Merging them
	// once after all readers finish avoids locking the per-entry hot path.
	workerSurveys := make([]*SurvivorSurvey, compactionManifestWorkerCount(len(dataManifests)))
	for i := range workerSurveys {
		workerSurveys[i] = NewSurvivorSurvey()
	}
	err = walkCompactionManifests(ctx, dataManifests,
		func(readCtx context.Context, worker, _ int, m iceberg.ManifestFile) error {
			survey := workerSurveys[worker]
			for entry, readErr := range m.Entries(fs, true) {
				if readErr != nil {
					return readErr
				}
				if err := readCtx.Err(); err != nil {
					return err
				}
				df := entry.DataFile()
				if df.ContentType() != iceberg.EntryContentData {
					continue
				}
				if _, beingRewritten := rewrittenPaths[df.FilePath()]; beingRewritten {
					continue
				}
				partition := internal.BorrowedDataFilePartition(df)
				if specs == nil {
					survey.AddSurvivor(partition, entry.SequenceNum())
				} else {
					survey.AddSurvivorWithSpec(df.SpecID(), partition, entry.SequenceNum())
				}
			}

			return nil
		})
	if err != nil {
		return nil, err
	}

	survey := NewSurvivorSurvey()
	for _, partial := range workerSurveys {
		survey.merge(partial)
	}

	if specs == nil {
		return DecideDeadEqualityDeletes(survey, candidates), nil
	}

	return DecideDeadEqualityDeletesWithSpecs(survey, candidates, specs)
}

// merge combines independent minima without changing conservative or spec-aware
// applicability. It is only called after the workers have finished.
func (s *SurvivorSurvey) merge(other *SurvivorSurvey) {
	s.EmptyPartMinSeq = min(s.EmptyPartMinSeq, other.EmptyPartMinSeq)
	s.minSeq = min(s.minSeq, other.minSeq)
	for key, seq := range other.PartMinSeq {
		updatePartitionMinSeq(s.PartMinSeq, key, seq)
	}
	for key, seq := range other.specPartMinSeq {
		updatePartitionMinSeq(s.specPartMinSeq, key, seq)
	}
}

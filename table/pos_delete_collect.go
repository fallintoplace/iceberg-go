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

	"golang.org/x/sync/errgroup"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// CollectDeadPositionDeletes walks the given snapshot's manifests and returns
// the classic position-delete files made dead by a rewrite that removes
// rewrittenPaths. A returned delete references only data files the rewrite is
// removing, so it is safe to expunge in the same commit.
//
// A file-scoped delete is resolved through referenced_data_file or equal
// file_path bounds. A partition-scoped delete is retained when a surviving
// data file in the same (specID, partition) has a sequence number to which the
// delete still applies.
//
// Deletion vectors are intentionally excluded: they are one-to-one with their
// referenced data file and are removed from a rewrite using the task result
// that identified the rewritten file.
func CollectDeadPositionDeletes(
	ctx context.Context,
	fs iceio.IO,
	snap *Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	if snap == nil || len(rewrittenPaths) == 0 {
		return nil, nil
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	candidates, hasPartitionScoped, err := collectPositionDeleteCandidates(ctx, fs, manifests)
	if err != nil {
		return nil, err
	}

	var minSurvivorSeq map[string]int64
	if hasPartitionScoped {
		minSurvivorSeq, err = minSurvivorSeqByPartition(ctx, fs, manifests, rewrittenPaths)
		if err != nil {
			return nil, err
		}
	}

	return decideDeadPositionDeletes(candidates, rewrittenPaths, minSurvivorSeq), nil
}

type positionDeleteCandidate struct {
	df               iceberg.DataFile
	fileScopedTarget string
	partitionKey     string
	seq              int64
}

// deadPositionDeleteManifestConcurrency bounds manifest reads and their
// per-manifest decoding state, even for snapshots with thousands of manifests.
const deadPositionDeleteManifestConcurrency = 8

// visitDeadPositionDeleteManifests decodes manifests concurrently, then merges
// each result in manifest-list order. This preserves first-seen delete-file
// deduplication and keeps at most one worker batch's decoded entries live.
func visitDeadPositionDeleteManifests[T any](
	ctx context.Context,
	manifests []iceberg.ManifestFile,
	content iceberg.ManifestContent,
	read func(context.Context, iceberg.ManifestFile) (T, error),
	merge func(T) error,
) error {
	processBatch := func(batch []iceberg.ManifestFile) error {
		if len(batch) == 1 {
			value, err := read(ctx, batch[0])
			if err != nil {
				return err
			}

			return merge(value)
		}

		results := make([]T, len(batch))
		group, groupCtx := errgroup.WithContext(ctx)
		for index, manifest := range batch {
			group.Go(func() error {
				value, err := read(groupCtx, manifest)
				results[index] = value

				return err
			})
		}
		if err := group.Wait(); err != nil {
			return err
		}

		for _, result := range results {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := merge(result); err != nil {
				return err
			}
		}

		return nil
	}

	batch := make([]iceberg.ManifestFile, 0, deadPositionDeleteManifestConcurrency)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return err
		}
		if manifest.ManifestContent() != content {
			continue
		}

		batch = append(batch, manifest)
		if len(batch) == deadPositionDeleteManifestConcurrency {
			if err := processBatch(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		return processBatch(batch)
	}

	return nil
}

type positionDeleteManifestEntry struct {
	df  iceberg.DataFile
	seq int64
}

func collectPositionDeleteCandidates(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile) (candidates []positionDeleteCandidate, hasPartitionScoped bool, err error) {
	// Read and filter each manifest independently. Resolve partition keys only
	// after merging in input order so a duplicate file never changes behavior.
	read := func(ctx context.Context, m iceberg.ManifestFile) ([]positionDeleteManifestEntry, error) {
		var entries []positionDeleteManifestEntry
		entryIndex := 0
		for entry, readErr := range m.Entries(fs, true) {
			if readErr != nil {
				return nil, readErr
			}
			if entryIndex&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			entryIndex++

			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentPosDeletes || IsDeletionVector(df) {
				continue
			}

			entries = append(entries, positionDeleteManifestEntry{
				df:  df,
				seq: entry.SequenceNum(),
			})
		}

		return entries, ctx.Err()
	}

	seen := make(map[string]struct{})
	merge := func(entries []positionDeleteManifestEntry) error {
		for _, entry := range entries {
			df := entry.df
			path := df.FilePath()
			if _, ok := seen[path]; ok {
				continue
			}
			seen[path] = struct{}{}

			if target := referencedDataFilePath(df); target != "" {
				candidates = append(candidates, positionDeleteCandidate{df: df, fileScopedTarget: target})

				continue
			}

			// An unset delete sequence cannot be compared safely. Retain it.
			if entry.seq >= 0 {
				partitionKey, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
				if err != nil {
					return fmt.Errorf("building partition key for position delete %s (spec %d): %w", df.FilePath(), df.SpecID(), err)
				}
				hasPartitionScoped = true
				candidates = append(candidates, positionDeleteCandidate{
					df:           df,
					partitionKey: partitionKey,
					seq:          entry.seq,
				})
			}
		}

		return nil
	}

	if err := visitDeadPositionDeleteManifests(ctx, manifests, iceberg.ManifestContentDeletes, read, merge); err != nil {
		return nil, false, err
	}

	return candidates, hasPartitionScoped, nil
}

func minSurvivorSeqByPartition(ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile, rewrittenPaths map[string]struct{}) (map[string]int64, error) {
	// Partition minima are associative, so each worker can reduce its entries
	// before publishing the result to the single ordered merge.
	read := func(ctx context.Context, m iceberg.ManifestFile) (map[string]int64, error) {
		minSeq := make(map[string]int64)
		entryIndex := 0
		for entry, readErr := range m.Entries(fs, true) {
			if readErr != nil {
				return nil, readErr
			}
			if entryIndex&1023 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			entryIndex++

			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if _, rewritten := rewrittenPaths[df.FilePath()]; rewritten {
				continue
			}

			// An unknown sequence is conservatively treated as zero.
			seq := max(entry.SequenceNum(), 0)
			key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return nil, fmt.Errorf("building partition key for surviving data file %s (spec %d): %w", df.FilePath(), df.SpecID(), err)
			}
			if cur, ok := minSeq[key]; ok {
				seq = min(seq, cur)
			}
			minSeq[key] = seq
		}

		return minSeq, ctx.Err()
	}

	result := make(map[string]int64)
	merge := func(partial map[string]int64) error {
		for key, seq := range partial {
			if cur, ok := result[key]; ok {
				seq = min(seq, cur)
			}
			result[key] = seq
		}

		return nil
	}

	if err := visitDeadPositionDeleteManifests(ctx, manifests, iceberg.ManifestContentData, read, merge); err != nil {
		return nil, err
	}

	return result, nil
}

func decideDeadPositionDeletes(candidates []positionDeleteCandidate, rewrittenPaths map[string]struct{}, minSurvivorSeq map[string]int64) []iceberg.DataFile {
	dead := make([]iceberg.DataFile, 0, len(candidates))
	for _, c := range candidates {
		if c.fileScopedTarget != "" {
			if _, rewritten := rewrittenPaths[c.fileScopedTarget]; rewritten {
				dead = append(dead, c.df)
			}

			continue
		}
		if cur, ok := minSurvivorSeq[c.partitionKey]; !ok || cur > c.seq {
			dead = append(dead, c.df)
		}
	}

	return dead
}

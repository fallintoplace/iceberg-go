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
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
)

// SurvivorSurvey tracks the lowest sequence number among surviving data files
// by partition spec and partition values. An unpartitioned equality delete
// applies to all survivors, so the global minimum is retained too.
type SurvivorSurvey struct {
	minSeq         int64
	specPartMinSeq map[string]int64
}

// PartitionSpecLookup resolves the partition spec used to write a data file.
type PartitionSpecLookup interface {
	PartitionSpecByID(int) *iceberg.PartitionSpec
}

// NewSurvivorSurvey returns an initially empty survey.
func NewSurvivorSurvey() *SurvivorSurvey {
	return &SurvivorSurvey{
		minSeq:         math.MaxInt64,
		specPartMinSeq: make(map[string]int64),
	}
}

// AddSurvivor records a surviving data file's spec ID, partition, and sequence.
// A negative sequence is treated as zero to avoid prematurely deleting files.
func (s *SurvivorSurvey) AddSurvivor(specID int32, partition map[int]any, seq int64) {
	if seq < 0 {
		seq = 0
	}

	s.minSeq = min(seq, s.minSeq)
	updatePartitionMinSeq(s.specPartMinSeq, partitionBucketKey(specID, partition), seq)
}

func (s *SurvivorSurvey) specAwareApplicableMinSeq(
	eqSpecID int32,
	eqPartition map[int]any,
	isUnpartitioned bool,
) int64 {
	if isUnpartitioned {
		return s.minSeq
	}
	if v, ok := s.specPartMinSeq[partitionBucketKey(eqSpecID, eqPartition)]; ok {
		return v
	}

	return math.MaxInt64
}

// DecideDeadEqualityDeletes is the exact spec-aware predicate for
// equality-delete cleanup during compaction. Given a survey of surviving data
// file sequences populated with [SurvivorSurvey.AddSurvivor], it
// returns the equality-delete files that no surviving data file could apply to.
//
// The cleanup rule is:
//
//	E applies to D iff E.seq > D.seq AND (
//	    E.spec.isUnpartitioned() ||
//	    (E.specID == D.specID && E.partition == D.partition)
//	)
//
// E is dead iff no applicable surviving D has D.seq < E.seq —
// equivalently, the applicable min-seq is >= E.seq.
//
// Defensive: candidates with sequence number < 0 (sentinel for unset)
// are skipped — preserved rather than risk dropping an unidentifiable
// file.
//
// Dedup by file path: the same eq-delete file may appear in multiple
// manifest entries after manifest merging.
func DecideDeadEqualityDeletes(
	survey *SurvivorSurvey,
	candidates []iceberg.ManifestEntry,
	specs PartitionSpecLookup,
) ([]iceberg.DataFile, error) {
	if survey == nil || len(candidates) == 0 {
		return nil, nil
	}
	if specs == nil {
		return nil, fmt.Errorf("%w: partition spec lookup is required", iceberg.ErrInvalidArgument)
	}

	dead := make([]iceberg.DataFile, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	unpartitionedBySpecID := make(map[int32]bool)
	for _, e := range candidates {
		df := e.DataFile()
		path := df.FilePath()
		if _, ok := seen[path]; ok {
			continue
		}
		if e.DataSequenceNumber() < 0 {
			continue
		}
		isUnpartitioned, ok := unpartitionedBySpecID[df.SpecID()]
		if !ok {
			spec := specs.PartitionSpecByID(int(df.SpecID()))
			if spec == nil {
				return nil, fmt.Errorf("deciding equality delete file %s: partition spec ID %d not found",
					path, df.SpecID())
			}
			isUnpartitioned = spec.IsUnpartitioned()
			unpartitionedBySpecID[df.SpecID()] = isUnpartitioned
		}
		if survey.specAwareApplicableMinSeq(
			df.SpecID(), internal.BorrowedDataFilePartition(df), isUnpartitioned,
		) >= e.DataSequenceNumber() {
			seen[path] = struct{}{}
			dead = append(dead, df)
		}
	}

	return dead, nil
}

// partitionBucketKey returns a deterministic string key for a
// (specID, partition) tuple. Used by the planner to group tasks for
// bin-packing — different specs must NOT mix because a compacted
// output file inherits a single spec.
func partitionBucketKey(specID int32, part map[int]any) string {
	if len(part) == 0 {
		return fmt.Sprintf("%d:_", specID)
	}

	return partitionBucketKeyFromTuple(specID, appendPartitionTuple(nil, part))
}

func partitionBucketKeyFromTuple(specID int32, tuple []byte) string {
	key := make([]byte, 0, len(tuple)+12)
	key = strconv.AppendInt(key, int64(specID), 10)
	key = append(key, ':')

	return string(append(key, tuple...))
}

// appendPartitionTuple emits the sorted "id=value;" tuple into dst.
// Caller is responsible for any leading prefix (e.g. specID).
func appendPartitionTuple(dst []byte, part map[int]any) []byte {
	var inlineIDs [8]int
	ids := inlineIDs[:0]
	if len(part) > len(inlineIDs) {
		ids = make([]int, 0, len(part))
	}
	for id := range part {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	if cap(dst) == 0 && len(part) != 0 {
		dst = make([]byte, 0, len(part)*16)
	}
	for _, id := range ids {
		dst = strconv.AppendInt(dst, int64(id), 10)
		dst = append(dst, '=')
		// Match fmt's %v output exactly; these partition keys are externally visible.
		switch value := part[id].(type) {
		case nil:
			dst = append(dst, "<nil>"...)
		case string:
			dst = append(dst, value...)
		case int32:
			dst = strconv.AppendInt(dst, int64(value), 10)
		case iceberg.Date:
			dst = strconv.AppendInt(dst, int64(value), 10)
		case int64:
			dst = strconv.AppendInt(dst, value, 10)
		case bool:
			dst = strconv.AppendBool(dst, value)
		default:
			dst = fmt.Append(dst, value)
		}
		dst = append(dst, ';')
	}

	return dst
}

func updatePartitionMinSeq(partitions map[string]int64, key string, seq int64) {
	if cur, ok := partitions[key]; ok {
		seq = min(seq, cur)
	}
	partitions[key] = seq
}

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
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// CollectDeadEqualityDeletes returns equality-delete files that cannot apply
// to any surviving data file after a rewrite removes rewrittenPaths.
// Partition specs and data sequence numbers determine applicability;
// unpartitioned equality deletes can apply to data in any partition.
//
// The scan walks delete manifests first and skips data manifests when no
// equality-delete candidates exist. The returned files can be removed in
// the same commit as the rewrite.
func CollectDeadEqualityDeletes(
	ctx context.Context,
	fs iceio.IO,
	specs PartitionSpecLookup,
	snap *table.Snapshot,
	rewrittenPaths map[string]struct{},
) ([]iceberg.DataFile, error) {
	if snap == nil || len(rewrittenPaths) == 0 {
		return nil, nil
	}
	if specs == nil {
		return nil, fmt.Errorf("%w: partition spec lookup is required", iceberg.ErrInvalidArgument)
	}

	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, err
	}

	// First pass: delete manifests only, collecting eq-delete candidates.
	var candidates []iceberg.ManifestEntry
	for _, m := range manifests {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if m.ManifestContent() != iceberg.ManifestContentDeletes {
			continue
		}
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			if e.DataFile().ContentType() != iceberg.EntryContentEqDeletes {
				continue
			}
			candidates = append(candidates, e)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	// Second pass: data manifests, building the survivor survey.
	survey := NewSurvivorSurvey()
	for _, m := range manifests {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		if m.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for e, err := range m.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := e.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if _, beingRewritten := rewrittenPaths[df.FilePath()]; beingRewritten {
				continue
			}
			partition := internal.BorrowedDataFilePartition(df)
			survey.AddSurvivor(df.SpecID(), partition, e.DataSequenceNumber())
		}
	}

	return DecideDeadEqualityDeletes(survey, candidates, specs)
}

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
	"iter"
	"sync"

	"github.com/apache/iceberg-go"
)

// Prefetch a bounded batch of manifest lists so remote object-store latency
// overlaps across snapshots. Results are yielded in snapshot order, and the
// next batch is not loaded until the consumer finishes the current one.
const inspectSnapshotManifestConcurrency = 8

type inspectSnapshotManifestResult struct {
	manifests iter.Seq[iceberg.ManifestFile]
	err       error
}

func prefetchInspectSnapshotManifests(
	ctx context.Context,
	provider ManifestProvider,
	snapshots []Snapshot,
) iter.Seq2[Snapshot, inspectSnapshotManifestResult] {
	return func(yield func(Snapshot, inspectSnapshotManifestResult) bool) {
		for start := 0; start < len(snapshots); start += inspectSnapshotManifestConcurrency {
			batch := snapshots[start:min(start+inspectSnapshotManifestConcurrency, len(snapshots))]
			if err := ctx.Err(); err != nil {
				yield(batch[0], inspectSnapshotManifestResult{err: err})

				return
			}

			if len(batch) == 1 {
				manifests, err := provider.Manifests(ctx, batch[0])
				yield(batch[0], inspectSnapshotManifestResult{manifests: manifests, err: err})

				return
			}

			results := make([]inspectSnapshotManifestResult, len(batch))
			var wg sync.WaitGroup
			wg.Add(len(batch))
			for index, snapshot := range batch {
				go func(index int, snapshot Snapshot) {
					defer wg.Done()

					results[index].manifests, results[index].err = provider.Manifests(ctx, snapshot)
				}(index, snapshot)
			}
			wg.Wait()

			for index, snapshot := range batch {
				if err := ctx.Err(); err != nil {
					yield(snapshot, inspectSnapshotManifestResult{err: err})

					return
				}
				if !yield(snapshot, results[index]) || results[index].err != nil {
					return
				}
			}
		}
	}
}

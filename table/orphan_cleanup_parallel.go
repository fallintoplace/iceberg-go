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
	"sync"

	"github.com/apache/iceberg-go"
)

// readUniqueSnapshotManifests reads manifest lists with bounded concurrency.
// The consumer deduplicates results as they arrive, rather than retaining all
// snapshot lists at once. Both workers and the result channel are bounded.
func readUniqueSnapshotManifests(
	ctx context.Context,
	snapshots []Snapshot,
	maxConcurrency int,
	load func(Snapshot) ([]iceberg.ManifestFile, error),
) (map[string]iceberg.ManifestFile, error) {
	unique := make(map[string]iceberg.ManifestFile)
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	active := 0
	for _, s := range snapshots {
		if s.ManifestList != "" {
			active++
		}
	}
	if active == 0 {
		return unique, nil
	}

	workers := min(max(maxConcurrency, 1), active)
	// Avoid goroutine overhead for a serial scan or a single snapshot.
	if workers == 1 {
		for _, snapshot := range snapshots {
			if snapshot.ManifestList == "" {
				continue
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			manifests, err := load(snapshot)
			if err != nil {
				return nil, fmt.Errorf("failed to read manifests for snapshot %d: %w", snapshot.SnapshotID, err)
			}
			for _, manifest := range manifests {
				path := manifest.FilePath()
				if _, exists := unique[path]; !exists {
					unique[path] = manifest
				}
			}
		}

		return unique, nil
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan Snapshot)
	results := make(chan []iceberg.ManifestFile, workers)
	firstError := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			for snapshot := range jobs {
				if runCtx.Err() != nil {
					return
				}
				manifests, err := load(snapshot)
				if err != nil {
					select {
					case firstError <- fmt.Errorf("failed to read manifests for snapshot %d: %w", snapshot.SnapshotID, err):
					default:
					}
					cancel()

					return
				}
				select {
				case results <- manifests:
				case <-runCtx.Done():
					return
				}
			}
		}()
	}

	// The producer is separate so a worker can wait for the consumer when
	// the bounded result channel fills without deadlocking the job feeder.
	go func() {
		defer close(jobs)
		for _, snapshot := range snapshots {
			if snapshot.ManifestList == "" {
				continue
			}
			select {
			case jobs <- snapshot:
			case <-runCtx.Done():
				return
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	for manifests := range results {
		for _, manifest := range manifests {
			path := manifest.FilePath()
			if _, exists := unique[path]; !exists {
				unique[path] = manifest
			}
		}
	}
	select {
	case err := <-firstError:
		return nil, err
	default:
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return unique, nil
}

// A manifest can contain millions of entries. Bound each reader's temporary
// path buffer independently of the number of entries it encounters.
const manifestReferenceBatchSize = 512

type referencePathBatch struct {
	paths      []string
	referenced map[string]bool
	mu         *sync.Mutex
}

func newReferencePathBatch(referenced map[string]bool, mu *sync.Mutex) *referencePathBatch {
	return &referencePathBatch{
		paths:      make([]string, 0, manifestReferenceBatchSize),
		referenced: referenced,
		mu:         mu,
	}
}

func (b *referencePathBatch) add(path string) {
	b.paths = append(b.paths, path)
	if len(b.paths) == cap(b.paths) {
		b.flush()
	}
}

func (b *referencePathBatch) flush() {
	if len(b.paths) == 0 {
		return
	}
	b.mu.Lock()
	for _, path := range b.paths {
		b.referenced[path] = true
	}
	b.mu.Unlock()
	b.paths = b.paths[:0]
}

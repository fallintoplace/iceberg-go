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
	"sync"
	"sync/atomic"
)

const maxCompactionManifestReaders = 8

func compactionManifestWorkerCount(manifests int) int {
	return min(manifests, maxCompactionManifestReaders)
}

// walkCompactionManifests processes each manifest exactly once. The worker ID
// lets callers maintain independent aggregation state without synchronizing
// each entry. Manifest indices preserve input ordering when collecting results.
func walkCompactionManifests[M any](
	ctx context.Context, manifests []M, visit func(context.Context, int, int, M) error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(manifests) == 0 {
		return nil
	}
	if len(manifests) == 1 {
		return visit(ctx, 0, 0, manifests[0])
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var next atomic.Int64
	var workers sync.WaitGroup
	var failureOnce sync.Once
	var failure error

	for worker := range compactionManifestWorkerCount(len(manifests)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				if runCtx.Err() != nil {
					return
				}
				index := int(next.Add(1)) - 1
				if index >= len(manifests) {
					return
				}
				if err := visit(runCtx, worker, index, manifests[index]); err != nil {
					failureOnce.Do(func() {
						failure = err
						cancel()
					})
					return
				}
			}
		}()
	}
	workers.Wait()
	if failure != nil {
		return failure
	}
	return ctx.Err()
}

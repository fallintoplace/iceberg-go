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

package maintenance

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReferencePathBatch(t *testing.T) {
	for _, n := range []int{0, 1, 511, 512, 513, 1024, 1031} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			refs := map[string]bool{"existing": false}
			var mu sync.Mutex
			batch := referencePathBatch{referenced: refs, mu: &mu}
			for i := range n {
				batch.add(strconv.Itoa(i))
			}
			batch.flush()
			batch.flush()
			require.Len(t, refs, n+1)
			require.False(t, refs["existing"])
			for i := range n {
				require.True(t, refs[strconv.Itoa(i)])
			}
			require.Empty(t, batch.paths)
		})
	}
}

func BenchmarkReferencePathBatch(b *testing.B) {
	for _, count := range []int{1, 32, 512, 65536} {
		paths := make([]string, count)
		for i := range paths {
			paths[i] = strconv.Itoa(i)
		}
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			for _, mode := range []string{"baseline", "batch"} {
				b.Run(mode, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						refs := make(map[string]bool)
						var mu sync.Mutex
						if mode == "baseline" {
							type refEntry struct {
								path   string
								isData bool
							}
							var entries []refEntry
							for _, p := range paths {
								entries = append(entries, refEntry{p, true})
							}
							mu.Lock()
							for _, e := range entries {
								refs[e.path] = e.isData
							}
							mu.Unlock()
						} else {
							batch := referencePathBatch{referenced: refs, mu: &mu}
							for _, p := range paths {
								batch.add(p)
							}
							batch.flush()
						}
						if len(refs) != count {
							b.Fatal(len(refs))
						}
					}
				})
			}
		})
	}
}

func TestReferencePathBatchConcurrent(t *testing.T) {
	const workers = 4
	const perWorker = referencePathBatchSize + 1
	refs := make(map[string]bool)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			batch := referencePathBatch{referenced: refs, mu: &mu}
			for i := range perWorker {
				batch.add(fmt.Sprintf("%d-%d", worker, i))
			}
			batch.flush()
		})
	}
	wg.Wait()
	require.Len(t, refs, workers*perWorker)
}

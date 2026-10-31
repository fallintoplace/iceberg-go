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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// Some tracking test filesystems record Open calls in a plain map. Serialize
// just Open for those fixtures while exercising concurrent manifest-list reads.
type synchronizedOpenIO struct {
	iceio.IO
	mu sync.Mutex
}

func (s *synchronizedOpenIO) Open(name string) (iceio.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.IO.Open(name)
}

func orphanBenchmarkSnapshots(count int) []Snapshot {
	snapshots := make([]Snapshot, count)
	for i := range snapshots {
		snapshots[i] = Snapshot{
			SnapshotID:   int64(i + 1),
			ManifestList: fmt.Sprintf("manifest-list-%d.avro", i),
		}
	}
	return snapshots
}

func TestReadUniqueSnapshotManifestsBounded(t *testing.T) {
	snapshots := orphanBenchmarkSnapshots(96)
	var active, peak atomic.Int32
	load := func(snapshot Snapshot) ([]iceberg.ManifestFile, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(time.Millisecond)
		return []iceberg.ManifestFile{
			iceberg.NewManifestFile(2, fmt.Sprintf("shared-%d.avro", snapshot.SnapshotID%12), 100, 0, 1).Build(),
		}, nil
	}

	got, err := readUniqueSnapshotManifests(t.Context(), snapshots, 8, load)
	require.NoError(t, err)
	require.Len(t, got, 12)
	require.Greater(t, peak.Load(), int32(1))
	require.LessOrEqual(t, peak.Load(), int32(8))
	require.Equal(t, int32(0), active.Load())

	peak.Store(0)
	got, err = readUniqueSnapshotManifests(t.Context(), snapshots, 1, load)
	require.NoError(t, err)
	require.Len(t, got, 12)
	require.Equal(t, int32(1), peak.Load())
}

func TestReadUniqueSnapshotManifestsErrorsAndEmptyLists(t *testing.T) {
	snapshots := orphanBenchmarkSnapshots(16)
	snapshots[0].ManifestList = ""
	sentinel := errors.New("failed manifest list")
	var calls atomic.Int32
	_, err := readUniqueSnapshotManifests(t.Context(), snapshots, 4,
		func(snapshot Snapshot) ([]iceberg.ManifestFile, error) {
			calls.Add(1)
			if snapshot.SnapshotID == 5 {
				return nil, sentinel
			}
			return nil, nil
		})
	require.ErrorIs(t, err, sentinel)
	require.ErrorContains(t, err, "snapshot 5")
	require.Greater(t, calls.Load(), int32(0))

	calls.Store(0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = readUniqueSnapshotManifests(ctx, snapshots, 8,
		func(Snapshot) ([]iceberg.ManifestFile, error) {
			calls.Add(1)
			return nil, nil
		})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(0), calls.Load())

	got, err := readUniqueSnapshotManifests(t.Context(),
		[]Snapshot{{SnapshotID: 1}}, 8,
		func(Snapshot) ([]iceberg.ManifestFile, error) {
			t.Fatal("empty manifest list must not be opened")
			return nil, nil
		})
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestReferencePathBatchConcurrent(t *testing.T) {
	referenced := map[string]bool{"metadata.json": false}
	var mu sync.Mutex
	var wg sync.WaitGroup
	const (
		workers = 8
		files   = 1025
	)
	wg.Add(workers)
	for worker := range workers {
		go func() {
			defer wg.Done()
			batch := newReferencePathBatch(referenced, &mu)
			for i := range files {
				batch.add(fmt.Sprintf("data-%d-%d", worker, i))
			}
			batch.flush()
		}()
	}
	wg.Wait()
	require.Len(t, referenced, 1+workers*files)
	require.False(t, referenced["metadata.json"])
	require.True(t, referenced["data-7-1024"])
}

var orphanReferenceBenchmarkSink int

// Compare the prior per-manifest accumulator to the production batcher.
// Both paths construct the same final reference map.
func BenchmarkOrphanReferencePathAccumulation(b *testing.B) {
	for _, count := range []int{65_536, 262_144} {
		paths := make([]string, count)
		for i := range paths {
			paths[i] = fmt.Sprint(i)
		}
		for _, mode := range []string{"buffer_all", "batched_512"} {
			b.Run(fmt.Sprintf("paths=%d/%s", count, mode), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					referenced := make(map[string]bool, count)
					var mu sync.Mutex
					if mode == "buffer_all" {
						var pending []string
						for _, path := range paths {
							pending = append(pending, path)
						}
						mu.Lock()
						for _, path := range pending {
							referenced[path] = true
						}
						mu.Unlock()
					} else {
						batch := newReferencePathBatch(referenced, &mu)
						for _, path := range paths {
							batch.add(path)
						}
						batch.flush()
					}
					orphanReferenceBenchmarkSink = len(referenced)
				}
			})
		}
	}
}

// A synthetic I/O benchmark that includes index construction and all
// snapshot-list reads. No actual object store, manifest-entry I/O, or deletion.
func BenchmarkOrphanSnapshotManifestLists(b *testing.B) {
	for _, count := range []int{64, 256} {
		snapshots := orphanBenchmarkSnapshots(count)
		for _, latency := range []time.Duration{0, time.Millisecond} {
			for _, workers := range []int{1, 8, 32} {
				for _, mode := range []string{"serial", "parallel"} {
					b.Run(fmt.Sprintf("snapshots=%d/latency=%s/workers=%d/%s",
						count, latency, workers, mode), func(b *testing.B) {
						load := func(Snapshot) ([]iceberg.ManifestFile, error) {
							if latency != 0 {
								time.Sleep(latency)
							}
							return []iceberg.ManifestFile{
								iceberg.NewManifestFile(2, "shared-manifest.avro", 100, 0, 1).Build(),
							}, nil
						}

						b.ReportAllocs()
						for i := 0; i < b.N; i++ {
							var got map[string]iceberg.ManifestFile
							var err error
							if mode == "parallel" {
								got, err = readUniqueSnapshotManifests(
									context.Background(), snapshots, workers, load)
							} else {
								got = make(map[string]iceberg.ManifestFile)
								for _, snapshot := range snapshots {
									manifests, readErr := load(snapshot)
									if readErr != nil {
										err = readErr
										break
									}
									for _, manifest := range manifests {
										if _, ok := got[manifest.FilePath()]; !ok {
											got[manifest.FilePath()] = manifest
										}
									}
								}
							}
							if err != nil || len(got) != 1 {
								b.Fatalf("manifest list results: %v (%d)", err, len(got))
							}
						}
					})
				}
			}
		}
	}
}

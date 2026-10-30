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
	"github.com/stretchr/testify/require"
)

func TestCollectIncrementalScanManifestsKeepsLatestDescriptor(t *testing.T) {
	snapshots := []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}, {SnapshotID: 3}}
	sets := map[int64]snapshotManifestSet{
		1: {all: []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "shared.avro", 10, 0, 1).Build(),
			iceberg.NewManifestBuilder(2, "first.avro", 10, 0, 1).Build(),
		}},
		2: {all: []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "shared.avro", 10, 0, 2).Build(),
		}},
		3: {all: []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, "last.avro", 10, 0, 3).Build(),
		}},
	}
	var examined []int64
	manifests, err := collectIncrementalScanManifests(t.Context(), snapshots, nil,
		func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			return sets[snapshot.SnapshotID], nil
		},
		func(manifest iceberg.ManifestFile) (bool, error) {
			examined = append(examined, manifest.SnapshotID())

			return true, nil
		})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 1, 2, 3}, examined)
	require.Len(t, manifests, 3)
	require.Equal(t, []string{"first.avro", "last.avro", "shared.avro"},
		[]string{manifests[0].FilePath(), manifests[1].FilePath(), manifests[2].FilePath()})
	require.EqualValues(t, 2, manifests[2].SnapshotID(), "later snapshot descriptor must win")
}

func TestCollectIncrementalScanManifestsChecksEveryDescriptor(t *testing.T) {
	snapshots := []Snapshot{{SnapshotID: 1}, {SnapshotID: 2}}
	wantErr := errors.New("invalid inherited manifest")
	_, err := collectIncrementalScanManifests(t.Context(), snapshots, nil,
		func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
			return snapshotManifestSet{all: []iceberg.ManifestFile{
				iceberg.NewManifestBuilder(2, "same-path.avro", 10, 0, snapshot.SnapshotID).Build(),
			}}, nil
		},
		func(manifest iceberg.ManifestFile) (bool, error) {
			if manifest.SnapshotID() == 2 {
				return false, wantErr
			}

			return true, nil
		})
	require.ErrorIs(t, err, wantErr)
}

func TestCollectIncrementalScanManifestsBoundsConcurrentReads(t *testing.T) {
	const count = 25
	snapshots := make([]Snapshot, count)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}

	started := make(chan int64, inspectSnapshotManifestPrefetch)
	release := make(chan struct{})
	var closeRelease sync.Once
	defer closeRelease.Do(func() { close(release) })

	var active, highWater atomic.Int32
	type result struct {
		manifests []iceberg.ManifestFile
		err       error
	}
	done := make(chan result, 1)
	go func() {
		manifests, err := collectIncrementalScanManifests(t.Context(), snapshots, nil,
			func(ctx context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
				current := active.Add(1)
				defer active.Add(-1)
				for {
					highest := highWater.Load()
					if current <= highest || highWater.CompareAndSwap(highest, current) {
						break
					}
				}
				if snapshot.SnapshotID <= inspectSnapshotManifestPrefetch {
					started <- snapshot.SnapshotID
				}

				select {
				case <-release:
				case <-ctx.Done():
					return snapshotManifestSet{}, ctx.Err()
				}

				return snapshotManifestSet{all: []iceberg.ManifestFile{
					iceberg.NewManifestBuilder(2,
						fmt.Sprintf("manifest-%02d.avro", snapshot.SnapshotID), 10, 0, snapshot.SnapshotID).Build(),
				}}, nil
			},
			func(iceberg.ManifestFile) (bool, error) { return true, nil })
		done <- result{manifests, err}
	}()

	// All eight reads must start before the first one is allowed to complete.
	for range inspectSnapshotManifestPrefetch {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("the initial manifest-list read window did not start")
		}
	}
	require.EqualValues(t, inspectSnapshotManifestPrefetch, highWater.Load())
	closeRelease.Do(func() { close(release) })
	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Len(t, got.manifests, count)
		for i, manifest := range got.manifests {
			require.Equal(t, fmt.Sprintf("manifest-%02d.avro", i+1), manifest.FilePath())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("incremental prefetch did not complete")
	}
	require.Zero(t, active.Load())
	require.LessOrEqual(t, highWater.Load(), int32(inspectSnapshotManifestPrefetch))
}

func TestCollectIncrementalScanManifestsUsesCompletedCache(t *testing.T) {
	ctx := t.Context()
	cache := newSnapshotManifestCache()
	snapshots := make([]Snapshot, 12)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
		snapshot := snapshots[i]
		_, err := cache.get(ctx, snapshot, func(context.Context) (snapshotManifestSet, error) {
			return snapshotManifestSet{all: []iceberg.ManifestFile{
				iceberg.NewManifestBuilder(2,
					fmt.Sprintf("cached-%02d.avro", snapshot.SnapshotID), 10, 0, snapshot.SnapshotID).Build(),
			}}, nil
		})
		require.NoError(t, err)
	}

	var coldLoads atomic.Int32
	manifests, err := collectIncrementalScanManifests(ctx, snapshots, cache,
		func(context.Context, Snapshot) (snapshotManifestSet, error) {
			coldLoads.Add(1)

			return snapshotManifestSet{}, errors.New("unexpected cold cache read")
		},
		func(iceberg.ManifestFile) (bool, error) { return true, nil })
	require.NoError(t, err)
	require.Len(t, manifests, len(snapshots))
	require.Zero(t, coldLoads.Load())
}

func TestCollectIncrementalScanManifestsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := collectIncrementalScanManifests(ctx,
		[]Snapshot{{SnapshotID: 1}}, nil,
		func(context.Context, Snapshot) (snapshotManifestSet, error) {
			t.Fatal("must not start a read with canceled context")

			return snapshotManifestSet{}, nil
		},
		func(iceberg.ManifestFile) (bool, error) { return true, nil })
	require.ErrorIs(t, err, context.Canceled)
}

func TestCollectIncrementalScanManifestsReadError(t *testing.T) {
	wantErr := errors.New("cannot read manifests")
	_, err := collectIncrementalScanManifests(t.Context(),
		[]Snapshot{{SnapshotID: 42}}, nil,
		func(context.Context, Snapshot) (snapshotManifestSet, error) {
			return snapshotManifestSet{}, wantErr
		},
		func(iceberg.ManifestFile) (bool, error) { return true, nil })
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "snapshot 42")
}

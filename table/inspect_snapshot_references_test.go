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
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func testReferencedManifestSet(snapshotID int64) snapshotManifestSet {
	return snapshotManifestSet{all: []iceberg.ManifestFile{
		iceberg.NewManifestFile(2, "shared.avro", 10, 0, snapshotID).Build(),
		iceberg.NewManifestFile(2, fmt.Sprintf("unique-%d.avro", snapshotID), 10, 0, snapshotID).Build(),
	}}
}

func TestStreamSnapshotManifestListsKeepsSharedReferences(t *testing.T) {
	snapshots := make([]Snapshot, 24)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}
	var active, maxActive atomic.Int32
	cached := func(s Snapshot) (snapshotManifestSet, bool) {
		if s.SnapshotID%2 == 0 {
			return testReferencedManifestSet(s.SnapshotID), true
		}

		return snapshotManifestSet{}, false
	}
	load := func(ctx context.Context, s Snapshot) (snapshotManifestSet, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			return snapshotManifestSet{}, ctx.Err()
		}

		return testReferencedManifestSet(s.SnapshotID), nil
	}

	for _, deduplicate := range []bool{false, true} {
		t.Run(fmt.Sprintf("deduplicate=%t", deduplicate), func(t *testing.T) {
			var got []string
			streamSnapshotManifestLists(t.Context(), snapshots, cached, load, deduplicate,
				func(s Snapshot, m iceberg.ManifestFile, err error) bool {
					require.NoError(t, err)
					got = append(got, fmt.Sprintf("%d:%s", s.SnapshotID, m.FilePath()))

					return true
				})
			want := make([]string, 0, 2*len(snapshots))
			for _, snapshot := range snapshots {
				if !deduplicate || snapshot.SnapshotID == 1 {
					want = append(want, fmt.Sprintf("%d:shared.avro", snapshot.SnapshotID))
				}
				want = append(want, fmt.Sprintf("%d:unique-%d.avro", snapshot.SnapshotID, snapshot.SnapshotID))
			}
			require.Equal(t, want, got)
		})
	}
	require.LessOrEqual(t, maxActive.Load(), int32(inspectSnapshotManifestPrefetch))
	require.Greater(t, maxActive.Load(), int32(1))
	require.Zero(t, active.Load())
}

func TestStreamSnapshotManifestListsErrorOrdering(t *testing.T) {
	snapshots := make([]Snapshot, 12)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}
	failure := errors.New("manifest list unavailable")
	var paths []string
	var gotErr error
	streamSnapshotManifestLists(t.Context(), snapshots, nil,
		func(_ context.Context, s Snapshot) (snapshotManifestSet, error) {
			if s.SnapshotID == 5 {
				return snapshotManifestSet{}, failure
			}

			return testReferencedManifestSet(s.SnapshotID), nil
		}, false, func(s Snapshot, m iceberg.ManifestFile, err error) bool {
			if err != nil {
				gotErr = err

				return false
			}
			paths = append(paths, fmt.Sprintf("%d:%s", s.SnapshotID, m.FilePath()))

			return true
		})
	require.Len(t, paths, 8)
	require.ErrorIs(t, gotErr, failure)
	require.ErrorContains(t, gotErr, "snapshot 5")
}

func TestStreamSnapshotManifestListsEarlyExitCancelsReads(t *testing.T) {
	snapshots := make([]Snapshot, 32)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
	}
	var active atomic.Int32
	count := 0
	streamSnapshotManifestLists(t.Context(), snapshots, nil,
		func(ctx context.Context, s Snapshot) (snapshotManifestSet, error) {
			active.Add(1)
			defer active.Add(-1)
			if s.SnapshotID > 1 {
				<-ctx.Done()
				return snapshotManifestSet{}, ctx.Err()
			}
			return testReferencedManifestSet(s.SnapshotID), nil
		}, false, func(_ Snapshot, _ iceberg.ManifestFile, err error) bool {
			require.NoError(t, err)
			count++

			return false
		})
	require.Equal(t, 1, count)
	require.Zero(t, active.Load())
}

var inspectAllManifestPrefetchBenchSink int

func BenchmarkAllManifestsListPrefetch(b *testing.B) {
	const snapshotsPerOp = 64
	snapshots := make([]Snapshot, snapshotsPerOp)
	sets := make([]snapshotManifestSet, snapshotsPerOp)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
		sets[i] = testReferencedManifestSet(int64(i + 1))
	}
	load := func(ctx context.Context, s Snapshot) (snapshotManifestSet, error) {
		select {
		case <-time.After(time.Millisecond):
		case <-ctx.Done():
			return snapshotManifestSet{}, ctx.Err()
		}

		return sets[s.SnapshotID-1], nil
	}
	cached := func(s Snapshot) (snapshotManifestSet, bool) {
		return sets[s.SnapshotID-1], true
	}

	for _, benchmark := range []struct {
		name       string
		prefetched bool
		warm       bool
	}{
		{name: "remote_sequential"},
		{name: "remote_prefetch8", prefetched: true},
		{name: "cached_sequential", warm: true},
		{name: "cached_prefetch8", warm: true, prefetched: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(snapshotsPerOp, "lists/op")
			for b.Loop() {
				count := 0
				if benchmark.prefetched {
					var tryCached func(Snapshot) (snapshotManifestSet, bool)
					if benchmark.warm {
						tryCached = cached
					}
					streamSnapshotManifestLists(b.Context(), snapshots, tryCached, load, false,
						func(_ Snapshot, _ iceberg.ManifestFile, err error) bool {
							if err != nil {
								b.Fatal(err)

								return false
							}
							count++

							return true
						})
				} else {
					for _, snapshot := range snapshots {
						var set snapshotManifestSet
						if benchmark.warm {
							set, _ = cached(snapshot)
						} else {
							var err error
							set, err = load(b.Context(), snapshot)
							if err != nil {
								b.Fatal(err)
							}
						}
						count += len(set.borrowAllManifests())
					}
				}
				if count != 2*snapshotsPerOp {
					b.Fatalf("got %d rows, want %d", count, 2*snapshotsPerOp)
				}
				inspectAllManifestPrefetchBenchSink = count
			}
		})
	}
}

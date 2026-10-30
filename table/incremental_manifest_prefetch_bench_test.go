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
	"slices"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
)

// BenchmarkIncrementalManifestPrefetch models object-store manifest-list reads
// with 1 ms latency. Each snapshot contributes one unique descriptor, so the
// benchmark isolates list-read scheduling instead of manifest decoding.
func BenchmarkIncrementalManifestPrefetch(b *testing.B) {
	const snapshotsCount = 64
	snapshots := make([]Snapshot, snapshotsCount)
	sets := make([]snapshotManifestSet, snapshotsCount)
	for i := range snapshots {
		snapshots[i].SnapshotID = int64(i + 1)
		sets[i] = snapshotManifestSet{all: []iceberg.ManifestFile{
			iceberg.NewManifestBuilder(2, fmt.Sprintf("manifest-%d.avro", i), 10, 0, int64(i+1)).Build(),
		}}
	}

	coldLoad := func(_ context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
		time.Sleep(time.Millisecond)

		return sets[snapshot.SnapshotID-1], nil
	}
	warmCache := newSnapshotManifestCache()
	for i, snapshot := range snapshots {
		value := sets[i]
		_, err := warmCache.get(context.Background(), snapshot,
			func(context.Context) (snapshotManifestSet, error) { return value, nil })
		if err != nil {
			b.Fatal(err)
		}
	}
	warmLoad := func(ctx context.Context, snapshot Snapshot) (snapshotManifestSet, error) {
		return warmCache.get(ctx, snapshot,
			func(context.Context) (snapshotManifestSet, error) {
				b.Fatal("warm manifest cache unexpectedly missed")

				return snapshotManifestSet{}, nil
			})
	}
	include := func(iceberg.ManifestFile) (bool, error) { return true, nil }

	for _, tc := range []struct {
		name     string
		prefetch bool
		cache    *snapshotManifestCache
		load     func(context.Context, Snapshot) (snapshotManifestSet, error)
	}{
		{"sequential/cold", false, nil, coldLoad},
		{"prefetch/cold", true, nil, coldLoad},
		{"sequential/warm", false, nil, warmLoad},
		{"prefetch/warm", true, warmCache, warmLoad},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			for b.Loop() {
				var (
					manifests []iceberg.ManifestFile
					err       error
				)
				if tc.prefetch {
					manifests, err = collectIncrementalScanManifests(ctx, snapshots, tc.cache, tc.load, include)
				} else {
					byPath := make(map[string]iceberg.ManifestFile)
					for _, snapshot := range snapshots {
						set, loadErr := tc.load(ctx, snapshot)
						if loadErr != nil {
							err = loadErr

							break
						}
						for _, manifest := range set.borrowAllManifests() {
							byPath[manifest.FilePath()] = manifest
						}
					}
					paths := make([]string, 0, len(byPath))
					for path := range byPath {
						paths = append(paths, path)
					}
					slices.Sort(paths)
					for _, path := range paths {
						manifests = append(manifests, byPath[path])
					}
				}
				if err != nil {
					b.Fatal(err)
				}
				if len(manifests) != snapshotsCount {
					b.Fatalf("expected %d manifests, got %d", snapshotsCount, len(manifests))
				}
			}
		})
	}
}

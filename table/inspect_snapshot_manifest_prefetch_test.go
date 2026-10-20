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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

func TestInspectAllManifestsPrefetchOrderAndConcurrency(t *testing.T) {
	const snapshotCount = 17

	var tracking *manifestTrackingIO
	tbl, want := tableWithManifestListsUsingIO(t, snapshotCount, func(memFS *iceio.MemFS) iceio.IO {
		tracking = &manifestTrackingIO{IO: memFS, delay: 5 * time.Millisecond}

		return tracking
	})

	reader, err := tbl.Inspect().AllManifests(t.Context())
	require.NoError(t, err)
	defer reader.Release()

	var got []string
	for reader.Next() {
		paths := reader.RecordBatch().Column(1).(*array.String)
		for index := range paths.Len() {
			got = append(got, paths.Value(index))
		}
	}
	require.NoError(t, reader.Err())
	require.Equal(t, want, got)

	tracking.mu.Lock()
	defer tracking.mu.Unlock()
	require.Greater(t, tracking.maxOpen, 1)
	require.LessOrEqual(t, tracking.maxOpen, inspectSnapshotManifestConcurrency)
}

type inspectFailingSnapshotListIO struct {
	iceio.IO
}

func (fs *inspectFailingSnapshotListIO) Open(path string) (iceio.File, error) {
	if strings.HasSuffix(path, "/snap-03.avro") || strings.HasSuffix(path, "/snap-07.avro") {
		return nil, fmt.Errorf("injected snapshot-list error for %s", path)
	}

	return fs.IO.Open(path)
}

func TestInspectAllManifestsPrefetchReportsFirstSnapshotError(t *testing.T) {
	tbl, _ := tableWithManifestListsUsingIO(t, 9, func(memFS *iceio.MemFS) iceio.IO {
		return &inspectFailingSnapshotListIO{IO: memFS}
	})

	reader, err := tbl.Inspect().AllManifests(t.Context())
	require.NoError(t, err)
	defer reader.Release()

	for reader.Next() {
	}
	require.ErrorContains(t, reader.Err(), "read snapshot 3 manifests")
}

type inspectCountingSnapshotListIO struct {
	iceio.IO
	opens atomic.Int32
}

func (fs *inspectCountingSnapshotListIO) Open(path string) (iceio.File, error) {
	if strings.Contains(path, "/metadata/snap-") {
		fs.opens.Add(1)
	}

	return fs.IO.Open(path)
}

func TestInspectSnapshotManifestPrefetchStopsBetweenBatches(t *testing.T) {
	var tracking *inspectCountingSnapshotListIO
	tbl, _ := tableWithManifestListsUsingIO(t, 24, func(memFS *iceio.MemFS) iceio.IO {
		tracking = &inspectCountingSnapshotListIO{IO: memFS}

		return tracking
	})

	count := 0
	for _, result := range prefetchInspectSnapshotManifests(t.Context(), tbl.ManifestProvider(), tbl.Metadata().Snapshots()) {
		require.NoError(t, result.err)
		count++

		break
	}
	require.Equal(t, 1, count)
	require.EqualValues(t, inspectSnapshotManifestConcurrency, tracking.opens.Load())
}

func BenchmarkInspectSnapshotManifestListPrefetch(b *testing.B) {
	const snapshots = 32

	tbl, _ := tableWithManifestListsUsingIO(b, snapshots, func(memFS *iceio.MemFS) iceio.IO {
		return &manifestTrackingIO{IO: memFS, delay: 10 * time.Millisecond}
	})
	// Disable caching to measure the manifest-list read path on every iteration.
	tbl.manifestCache = nil
	history := tbl.Metadata().Snapshots()

	b.Run("serial", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			provider := tbl.ManifestProvider()
			found := 0
			for _, snapshot := range history {
				manifests, err := provider.Manifests(b.Context(), snapshot)
				if err != nil {
					b.Fatal(err)
				}
				for range manifests {
					found++
				}
			}
			if found != snapshots {
				b.Fatalf("got %d manifests, want %d", found, snapshots)
			}
		}
	})

	b.Run("prefetch-8", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			provider := tbl.ManifestProvider()
			found := 0
			for _, result := range prefetchInspectSnapshotManifests(context.Background(), provider, history) {
				if result.err != nil {
					b.Fatal(result.err)
				}
				for range result.manifests {
					found++
				}
			}
			if found != snapshots {
				b.Fatalf("got %d manifests, want %d", found, snapshots)
			}
		}
	})
}

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
	"slices"

	"github.com/apache/iceberg-go"
)

// collectIncrementalScanManifests loads manifest lists with bounded prefetch
// and preserves their snapshot order. A later descriptor replaces an earlier
// one for the same path, matching incremental planning before prefetch.
// The selector sees every descriptor, including inherited delete manifests,
// so changelog validation cannot be bypassed by duplicate paths.
func collectIncrementalScanManifests(
	ctx context.Context,
	snapshots []Snapshot,
	cache *snapshotManifestCache,
	load func(context.Context, Snapshot) (snapshotManifestSet, error),
	include func(iceberg.ManifestFile) (bool, error),
) ([]iceberg.ManifestFile, error) {
	byPath := make(map[string]iceberg.ManifestFile)
	var collectErr error
	streamSnapshotManifestSets(ctx, snapshots, cache.tryGetComplete, load,
		func(set snapshotManifestSet, err error) bool {
			if err != nil {
				collectErr = err

				return false
			}
			for _, manifest := range set.borrowAllManifests() {
				keep, err := include(manifest)
				if err != nil {
					collectErr = err

					return false
				}
				if keep {
					byPath[manifest.FilePath()] = manifest
				}
			}

			return true
		})
	if collectErr != nil {
		return nil, collectErr
	}

	paths := make([]string, 0, len(byPath))
	for path := range byPath {
		paths = append(paths, path)
	}
	slices.Sort(paths)

	out := make([]iceberg.ManifestFile, 0, len(paths))
	for _, path := range paths {
		out = append(out, byPath[path])
	}

	return out, nil
}

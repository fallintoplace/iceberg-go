// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
	"context"
	"fmt"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
)

// AllManifests returns every manifest referenced by every snapshot currently
// tracked by the table. A manifest referenced by multiple snapshots produces
// one row per reference, identified by reference_snapshot_id.
// Results are streamed in bounded record batches while each snapshot's
// manifest list is traversed.
func (i InspectTable) AllManifests(ctx context.Context) (array.RecordReader, error) {
	arrowSchema, err := SchemaToArrowSchema(AllManifestsSchema(), nil, true, false)
	if err != nil {
		return nil, fmt.Errorf("inspect all manifests: build arrow schema: %w", err)
	}

	snapshots := i.tbl.Metadata().Snapshots()
	provider := i.tbl.ManifestProvider()

	return array.ReaderFromIter(arrowSchema, func(yield func(arrow.RecordBatch, error) bool) {
		bldr := array.NewRecordBuilder(i.alloc, arrowSchema)
		defer bldr.Release()

		rows := 0
		emitted := false
		chunk := make([]iceberg.ManifestFile, 0, inspectRecordBatchSize)
		var referenceSnapshotID int64
		emit := func() bool {
			if rows == 0 {
				return true
			}

			batch := bldr.NewRecordBatch()
			rows = 0
			emitted = true

			return yield(batch, nil)
		}
		emitEmpty := func() {
			batch := bldr.NewRecordBatch()
			emitted = true
			_ = yield(batch, nil)
		}
		yieldError := func(err error) {
			_ = yield(nil, err)
		}
		appendChunk := func() bool {
			if len(chunk) == 0 {
				return true
			}
			if err := i.appendManifestRows(ctx, bldr, slices.Values(chunk), &referenceSnapshotID); err != nil {
				yieldError(fmt.Errorf("inspect all manifests: snapshot %d: %w", referenceSnapshotID, err))

				return false
			}
			rows += len(chunk)
			chunk = chunk[:0]

			return true
		}

		completed := true
		// Unlike all_files, all_manifests must include one row for every
		// snapshot reference, including references to the same manifest path.
		streamSnapshotManifestLists(ctx, snapshots, provider.cachedSet, provider.set, false,
			func(snapshot Snapshot, manifest iceberg.ManifestFile, sourceErr error) bool {
				if sourceErr != nil {
					completed = false
					yieldError(fmt.Errorf("inspect all manifests: %w", sourceErr))

					return false
				}
				// Flush at snapshot boundaries so every row uses its own
				// reference_snapshot_id, even across prefetched lists.
				if len(chunk) > 0 && referenceSnapshotID != snapshot.SnapshotID {
					if !appendChunk() {
						completed = false

						return false
					}
				}
				referenceSnapshotID = snapshot.SnapshotID
				chunk = append(chunk, manifest)
				if rows+len(chunk) < inspectRecordBatchSize {
					return true
				}
				if !appendChunk() || !emit() {
					completed = false

					return false
				}

				return true
			})
		if !completed || !appendChunk() {
			return
		}
		if rows > 0 {
			_ = emit()
		} else if !emitted {
			emitEmpty()
		}
	}), nil
}

// AllManifestsSchema returns the schema of the all_manifests metadata table.
func AllManifestsSchema() *iceberg.Schema {
	fields := ManifestsSchema().Fields()
	fields = append(fields,
		iceberg.NestedField{
			ID: 18, Name: "reference_snapshot_id", Type: iceberg.PrimitiveTypes.Int64, Required: true,
		},
		iceberg.NestedField{
			ID: 19, Name: "key_metadata", Type: iceberg.PrimitiveTypes.Binary, Required: false,
		},
	)

	return iceberg.NewSchema(0, fields...)
}

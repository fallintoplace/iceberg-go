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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
)

// perFileDVBitmaps maps each data-file path to the deletion-vector bitmap
// that applies to it. Kept separate from perFilePosDeletes so the row-filter
// pipeline can use compute.Filter on a Boolean mask built from Contains()
// directly, instead of materializing positions into a set[int64] + Take.
type perFileDVBitmaps = map[string]*dv.RoaringPositionBitmap

// lazyDeletionVectorLoader indexes deletion-vector metadata for a scan, but
// waits to read a Puffin file until a task for one of its data files is
// processed. Each Puffin group is loaded once and all of its bitmaps are kept
// in the scan-scoped cache so tasks sharing a file do not repeat the read.
type lazyDeletionVectorLoader struct {
	fs iceio.IO

	byDataFile map[string]*lazyDeletionVectorGroup
}

type lazyDeletionVectorGroup struct {
	puffinPath          string
	referencedDataFiles []string
	files               []iceberg.DataFile

	once    sync.Once
	bitmaps perFileDVBitmaps
	err     error
}

func newLazyDeletionVectorLoader(fs iceio.IO, tasks []FileScanTask) (*lazyDeletionVectorLoader, error) {
	uniqueDVs, err := collectUniqueDeletionVectors(tasks)
	if err != nil {
		return nil, err
	}

	groups := groupDeletionVectors(uniqueDVs)
	loader := &lazyDeletionVectorLoader{
		fs:         fs,
		byDataFile: make(map[string]*lazyDeletionVectorGroup, len(uniqueDVs)),
	}
	for _, group := range groups {
		for _, ref := range group.referencedDataFiles {
			loader.byDataFile[ref] = group
		}
	}

	return loader, nil
}

func (l *lazyDeletionVectorLoader) load(ctx context.Context, dataFilePath string) (*dv.RoaringPositionBitmap, error) {
	if l == nil {
		return nil, nil
	}

	group := l.byDataFile[dataFilePath]
	if group == nil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	group.once.Do(func() {
		bitmaps, err := dv.ReadDVs(l.fs, group.files)
		if err != nil {
			group.err = fmt.Errorf("read deletion vectors from %s: %w", group.puffinPath, err)

			return
		}
		group.bitmaps = make(perFileDVBitmaps, len(bitmaps))
		for i, ref := range group.referencedDataFiles {
			group.bitmaps[ref] = bitmaps[i]
		}
	})

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if group.err != nil {
		return nil, group.err
	}

	return group.bitmaps[dataFilePath], nil
}

func collectUniqueDeletionVectors(tasks []FileScanTask) (map[string]iceberg.DataFile, error) {
	uniqueDVs := make(map[string]iceberg.DataFile)

	for _, t := range tasks {
		for _, d := range t.DeletionVectorFiles {
			_, _, ref, contentOffset, contentSize := iceinternal.BorrowedDataFilePointers(d)
			if ref == nil {
				return nil, fmt.Errorf("%w: deletion vector %s missing referenced_data_file",
					dv.ErrInvalidDeletionVector, d.FilePath())
			}
			if *ref == "" {
				return nil, fmt.Errorf("%w: deletion vector %s missing or empty referenced_data_file",
					dv.ErrInvalidDeletionVector, d.FilePath())
			}
			if contentOffset == nil || contentSize == nil {
				// Spec §Manifest Files: content_offset and content_size_in_
				// bytes are required for DV entries. Surface the missing-
				// field cause directly here — otherwise the dedup check
				// below would produce the misleading "multiple deletion
				// vectors" error when two equally-broken entries collide.
				return nil, fmt.Errorf("%w: deletion vector %s missing content_offset/content_size_in_bytes",
					dv.ErrInvalidDeletionVector, d.FilePath())
			}
			if existing, seen := uniqueDVs[*ref]; seen {
				if !sameDVBlob(existing, d) {
					return nil, fmt.Errorf(
						"multiple deletion vectors for data file %s: %s and %s",
						*ref, existing.FilePath(), d.FilePath())
				}

				continue
			}
			uniqueDVs[*ref] = d
		}
	}

	return uniqueDVs, nil
}

func groupDeletionVectors(uniqueDVs map[string]iceberg.DataFile) map[string]*lazyDeletionVectorGroup {
	groups := make(map[string]*lazyDeletionVectorGroup)
	for ref, dvFile := range uniqueDVs {
		group := groups[dvFile.FilePath()]
		if group == nil {
			group = &lazyDeletionVectorGroup{puffinPath: dvFile.FilePath()}
			groups[dvFile.FilePath()] = group
		}

		group.referencedDataFiles = append(group.referencedDataFiles, ref)
		group.files = append(group.files, dvFile)
	}

	return groups
}

// sameDVBlob reports whether two DV manifest entries point at the same puffin
// blob — identical puffin file path and content offset. Different of either
// means two distinct DVs for the same data file, the over-deletion case
// collectUniqueDeletionVectors rejects.
//
// Java's DeleteFileIndex is stricter: any second DV for the same data file is
// rejected with ValidationException("Can't index multiple DVs for %s"), even
// when both entries reference the same underlying blob. Same-blob dedup here
// is a deliberate divergence — reading the same blob twice is wasteful, not
// incorrect. ContentOffset is required to be non-nil by the spec and by the
// pre-pass in collectUniqueDeletionVectors, so the comparison below assumes
// both.
func sameDVBlob(a, b iceberg.DataFile) bool {
	if a.FilePath() != b.FilePath() {
		return false
	}

	_, _, _, aOffset, _ := iceinternal.BorrowedDataFilePointers(a)
	_, _, _, bOffset, _ := iceinternal.BorrowedDataFilePointers(b)

	return *aOffset == *bOffset
}

func deletionVectorKeepsRow(bitmap *dv.RoaringPositionBitmap, rowCount, pos int64) bool {
	return pos >= rowCount || pos < 0 || !bitmap.Contains(uint64(pos))
}

// filterByDeletionVector drops deleted rows without materializing a keep-mask
// for the whole file. Contiguous batches use a bit-packed mask for only their
// affected positions; clean batches are returned unchanged. Pruned row groups
// use the position cursor and build a mask only after finding a deleted row.
// Positions beyond a stale manifest row count are retained.
func filterByDeletionVector(ctx context.Context, bitmap *dv.RoaringPositionBitmap, rowCount int64, cursor *rowPositionCursor) recProcessFn {
	nextIdx := int64(0)
	mem := compute.GetAllocator(ctx)

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		nrows := r.NumRows()
		if !cursor.src.pruned() {
			currentIdx := nextIdx
			nextIdx += nrows

			if currentIdx >= rowCount {
				r.Retain()

				return r, nil
			}

			maskEnd := min(nextIdx, rowCount)
			keepBits := bitmap.KeepMaskForRange(uint64(currentIdx), uint64(maskEnd))
			if keepBits == nil {
				r.Retain()

				return r, nil
			}

			if nextIdx <= rowCount {
				mask := array.NewBoolean(int(nrows), memory.NewBufferBytes(keepBits), nil, 0)
				defer mask.Release()

				return compute.FilterRecordBatch(ctx, r, mask, compute.DefaultFilterOptions())
			}

			// A stale row count can end inside this batch. Leave the
			// out-of-range suffix untouched rather than slicing past it.
			bldr := array.NewBooleanBuilder(mem)
			defer bldr.Release()
			bldr.Reserve(int(nrows))
			for pos := currentIdx; pos < nextIdx; pos++ {
				bldr.Append(deletionVectorKeepsRow(bitmap, rowCount, pos))
			}
			mask := bldr.NewBooleanArray()
			defer mask.Release()

			return compute.FilterRecordBatch(ctx, r, mask, compute.DefaultFilterOptions())
		}

		var bldr *array.BooleanBuilder
		for i := range nrows {
			pos := cursor.next()
			keep := deletionVectorKeepsRow(bitmap, rowCount, pos)
			if bldr == nil {
				if keep {
					continue
				}
				bldr = array.NewBooleanBuilder(mem)
				bldr.Reserve(int(nrows))
				for range i {
					bldr.Append(true)
				}
			}
			bldr.Append(keep)
		}
		if bldr == nil {
			r.Retain()

			return r, nil
		}
		defer bldr.Release()
		mask := bldr.NewBooleanArray()
		defer mask.Release()

		return compute.FilterRecordBatch(ctx, r, mask, compute.DefaultFilterOptions())
	}
}

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
	"math"
	"math/bits"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table/dv"
)

const (
	// This must stay a power of two because appendFilePathChunk uses a bit mask
	// for periodic cancellation checks.
	positionalDeleteCancellationCheckInterval = 16 * 1024
)

func filePathValueType(dt arrow.DataType) arrow.DataType {
	if dictType, ok := dt.(*arrow.DictionaryType); ok {
		return dictType.ValueType
	}

	return dt
}

func filePathValues(values arrow.Array) (arrow.TypedArray[string], error) {
	switch arr := values.(type) {
	case *array.Dictionary:
		wrapped, err := array.NewDictWrapper[string](arr)
		if err != nil {
			return nil, fmt.Errorf("%w: file_path column is not string: %w",
				iceberg.ErrInvalidSchema, err)
		}

		return wrapped, nil
	case arrow.TypedArray[string]:
		return arr, nil
	default:
		return nil, fmt.Errorf("%w: unsupported file_path column type %s in position delete file",
			iceberg.ErrInvalidSchema, values.DataType())
	}
}

type posDeleteCursor struct {
	chunks     []*array.Int64
	chunkIndex int
	offset     int
}

func validatePositionDeletePositions(positions *array.Int64) error {
	for row := range positions.Len() {
		if position := positions.Value(row); position < 0 {
			return fmt.Errorf("%w: negative pos %d in position delete file",
				iceberg.ErrInvalidSchema, position)
		}
	}

	return nil
}

func newPosDeleteCursor(posCol *arrow.Chunked) (posDeleteCursor, error) {
	chunks := posCol.Chunks()
	arrays := make([]*array.Int64, len(chunks))
	for i, chunk := range chunks {
		posArr, ok := chunk.(*array.Int64)
		if !ok {
			return posDeleteCursor{}, fmt.Errorf("%w: unsupported pos chunk array type %T in position delete file",
				iceberg.ErrInvalidSchema, chunk)
		}
		arrays[i] = posArr
	}

	return posDeleteCursor{chunks: arrays}, nil
}

func (c *posDeleteCursor) next() (int64, bool) {
	for c.chunkIndex < len(c.chunks) && c.offset == c.chunks[c.chunkIndex].Len() {
		c.chunkIndex++
		c.offset = 0
	}
	if c.chunkIndex == len(c.chunks) {
		return 0, false
	}

	pos := c.chunks[c.chunkIndex].Value(c.offset)
	c.offset++

	return pos, true
}

type posDeleteAccumulator struct {
	mem      memory.Allocator
	targets  map[string]struct{}
	builders map[string]*array.Int64Builder
}

func newPosDeleteAccumulator(ctx context.Context, targets map[string]struct{}) *posDeleteAccumulator {
	return &posDeleteAccumulator{
		mem:      compute.GetAllocator(ctx),
		targets:  targets,
		builders: make(map[string]*array.Int64Builder),
	}
}

func (a *posDeleteAccumulator) release() {
	for _, builder := range a.builders {
		builder.Release()
	}
	a.builders = nil
}

func (a *posDeleteAccumulator) finish() map[string]*arrow.Chunked {
	if a.builders == nil {
		panic("position delete accumulator is already finished or released")
	}

	results := make(map[string]*arrow.Chunked, len(a.builders))
	for path, builder := range a.builders {
		positions := builder.NewInt64Array()
		builder.Release()

		results[path] = arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{positions})
		positions.Release()
	}
	a.builders = nil

	return results
}

func validatePosDeleteColumns(filePathType arrow.DataType, filePathNulls int,
	posType arrow.DataType, posNulls int,
) error {
	if filePathNulls > 0 {
		return fmt.Errorf("%w: null file_path in position delete file", iceberg.ErrInvalidSchema)
	}
	if filePathValueType(filePathType).ID() == arrow.STRING_VIEW {
		return fmt.Errorf("%w: unsupported file_path column type %s in position delete file",
			iceberg.ErrInvalidSchema, filePathType)
	}
	if posNulls > 0 {
		return fmt.Errorf("%w: null pos in position delete file", iceberg.ErrInvalidSchema)
	}
	if posType.ID() != arrow.INT64 {
		return fmt.Errorf("%w: unsupported pos column type %s in position delete file",
			iceberg.ErrInvalidSchema, posType)
	}

	return nil
}

func validatePosDeleteColumnLengths(filePathLen, posLen int) error {
	if filePathLen != posLen {
		return fmt.Errorf("%w: file_path and pos columns have different lengths: %d and %d",
			iceberg.ErrInvalidSchema, filePathLen, posLen)
	}

	return nil
}

// A position delete file usually references few data files. Do not reserve
// memory proportional to an unusually large Arrow dictionary.
const maxPositionDeleteDictionaryCacheEntries = 4096

func (a *posDeleteAccumulator) appendFilePathChunk(ctx context.Context, filePathChunk arrow.Array,
	posCursor *posDeleteCursor,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	paths, err := filePathValues(filePathChunk)
	if err != nil {
		return err
	}

	var dictionary arrow.Array
	var indices *array.Dictionary
	if dict, ok := filePathChunk.(*array.Dictionary); ok {
		indices = dict
		if dict.Dictionary().NullN() > 0 {
			dictionary = dict.Dictionary()
		}
		if size := dict.Dictionary().Len(); size > 0 && size <= maxPositionDeleteDictionaryCacheEntries {
			return a.appendFilePathChunkWithDictionaryCache(
				ctx, paths, dictionary, indices, dict, posCursor)
		}
	}

	return a.appendFilePathChunkWithoutCache(
		ctx, filePathChunk.Len(), paths, dictionary, indices, posCursor)
}

func (a *posDeleteAccumulator) appendFilePathChunkWithoutCache(
	ctx context.Context,
	length int,
	paths arrow.TypedArray[string],
	dictionary arrow.Array,
	indices *array.Dictionary,
	posCursor *posDeleteCursor,
) error {
	for i := range length {
		if i&(positionalDeleteCancellationCheckInterval-1) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		pos, ok := posCursor.next()
		if !ok {
			return fmt.Errorf("%w: position delete columns ended before file_path column",
				iceberg.ErrInvalidSchema)
		}
		if pos < 0 {
			return fmt.Errorf("%w: negative pos %d in position delete file",
				iceberg.ErrInvalidSchema, pos)
		}
		if indices != nil && indices.IsNull(i) {
			return fmt.Errorf("%w: null file_path dictionary index in position delete file",
				iceberg.ErrInvalidSchema)
		}
		if dictionary != nil && dictionary.IsNull(indices.GetValueIndex(i)) {
			return fmt.Errorf("%w: null file_path dictionary value in position delete file",
				iceberg.ErrInvalidSchema)
		}

		path := paths.Value(i)
		if len(a.targets) > 0 {
			if _, ok := a.targets[path]; !ok {
				continue
			}
		}

		builder, ok := a.builders[path]
		if !ok {
			path = strings.Clone(path)
			builder = array.NewInt64Builder(a.mem)
			a.builders[path] = builder
		}
		builder.Append(pos)
	}

	return nil
}

func (a *posDeleteAccumulator) appendFilePathChunkWithDictionaryCache(
	ctx context.Context,
	paths arrow.TypedArray[string],
	dictionary arrow.Array,
	indices *array.Dictionary,
	cachedIndices *array.Dictionary,
	posCursor *posDeleteCursor,
) error {
	cachedBuilders := make([]*array.Int64Builder, cachedIndices.Dictionary().Len())
	cachedSeen := make([]bool, len(cachedBuilders))

	for i := range cachedIndices.Len() {
		if i&(positionalDeleteCancellationCheckInterval-1) == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}

		pos, ok := posCursor.next()
		if !ok {
			return fmt.Errorf("%w: position delete columns ended before file_path column",
				iceberg.ErrInvalidSchema)
		}
		if pos < 0 {
			return fmt.Errorf("%w: negative pos %d in position delete file",
				iceberg.ErrInvalidSchema, pos)
		}
		if indices != nil && indices.IsNull(i) {
			return fmt.Errorf("%w: null file_path dictionary index in position delete file",
				iceberg.ErrInvalidSchema)
		}
		if dictionary != nil && dictionary.IsNull(indices.GetValueIndex(i)) {
			return fmt.Errorf("%w: null file_path dictionary value in position delete file",
				iceberg.ErrInvalidSchema)
		}

		// Resolve each dictionary index once per chunk. Several indexes may
		// refer to the same file path, so retain the existing string-keyed
		// builders as the source of truth.
		cacheIndex := cachedIndices.GetValueIndex(i)
		if cacheIndex >= 0 && cacheIndex < len(cachedSeen) && cachedSeen[cacheIndex] {
			if builder := cachedBuilders[cacheIndex]; builder != nil {
				builder.Append(pos)
			}

			continue
		}

		path := paths.Value(i)
		if len(a.targets) > 0 {
			if _, ok := a.targets[path]; !ok {
				// A nil builder caches a rejected target as well.
				if cacheIndex >= 0 && cacheIndex < len(cachedSeen) {
					cachedSeen[cacheIndex] = true
				}

				continue
			}
		}

		builder, ok := a.builders[path]
		if !ok {
			path = strings.Clone(path)
			builder = array.NewInt64Builder(a.mem)
			a.builders[path] = builder
		}
		if cacheIndex >= 0 && cacheIndex < len(cachedSeen) {
			cachedSeen[cacheIndex] = true
			cachedBuilders[cacheIndex] = builder
		}
		builder.Append(pos)
	}

	return nil
}

func (a *posDeleteAccumulator) appendChunked(ctx context.Context, filePathCol, posCol *arrow.Chunked) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validatePosDeleteColumns(filePathCol.DataType(), filePathCol.NullN(),
		posCol.DataType(), posCol.NullN()); err != nil {
		return err
	}
	if err := validatePosDeleteColumnLengths(filePathCol.Len(), posCol.Len()); err != nil {
		return err
	}

	posCursor, err := newPosDeleteCursor(posCol)
	if err != nil {
		return err
	}

	for _, filePathChunk := range filePathCol.Chunks() {
		if err := a.appendFilePathChunk(ctx, filePathChunk, &posCursor); err != nil {
			return err
		}
	}

	return ctx.Err()
}

// appendRecord requires columns projected in file_path, pos order.
func (a *posDeleteAccumulator) appendRecord(ctx context.Context, record arrow.RecordBatch) error {
	if record.NumCols() != 2 {
		return fmt.Errorf("%w: projected position delete record has %d columns, expected 2",
			iceberg.ErrInvalidSchema, record.NumCols())
	}

	filePathCol := record.Column(0)
	posCol := record.Column(1)
	if err := validatePosDeleteColumns(filePathCol.DataType(), filePathCol.NullN(),
		posCol.DataType(), posCol.NullN()); err != nil {
		return err
	}
	if err := validatePosDeleteColumnLengths(filePathCol.Len(), posCol.Len()); err != nil {
		return err
	}

	posArr, ok := posCol.(*array.Int64)
	if !ok {
		return fmt.Errorf("%w: unsupported pos record array type %T in position delete file",
			iceberg.ErrInvalidSchema, posCol)
	}
	posCursor := posDeleteCursor{chunks: []*array.Int64{posArr}}
	if err := a.appendFilePathChunk(ctx, filePathCol, &posCursor); err != nil {
		return err
	}

	return ctx.Err()
}

func groupPosDeletesByFilePath(ctx context.Context, filePathCol, posCol *arrow.Chunked) (results map[string]*arrow.Chunked, err error) {
	acc := newPosDeleteAccumulator(ctx, nil)
	defer func() {
		if err != nil {
			acc.release()
		}
	}()

	if err := acc.appendChunked(ctx, filePathCol, posCol); err != nil {
		return nil, err
	}

	return acc.finish(), nil
}

const maxPositionalDeletePreallocation = 64 * 1024

// collectPosDeletePositions serves producePosDeletesFromTask when a
// transaction creates new delete records. Scan tasks use cached bitmaps instead.
func collectPosDeletePositions(positionalDeletes positionDeletes) (set[int64], error) {
	totalPositions := 0
	for _, chunk := range positionalDeletes {
		if chunk == nil {
			continue
		}
		// The set only reserves a bounded hint. Stop summing once that
		// hint is reached so a large collection of chunks cannot overflow
		// int before the cap is applied.
		if chunk.Len() >= maxPositionalDeletePreallocation-totalPositions {
			totalPositions = maxPositionalDeletePreallocation

			break
		}
		totalPositions += chunk.Len()
	}

	deletes := make(set[int64], min(totalPositions, maxPositionalDeletePreallocation))
	for _, chunk := range positionalDeletes {
		if chunk == nil {
			return nil, fmt.Errorf("%w: nil pos column chunk in position delete file",
				iceberg.ErrInvalidSchema)
		}
		if chunk.DataType().ID() != arrow.INT64 {
			return nil, fmt.Errorf("%w: unsupported pos column type %s in position delete file",
				iceberg.ErrInvalidSchema, chunk.DataType())
		}
		for _, arr := range chunk.Chunks() {
			posArr, ok := arr.(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("%w: unsupported pos chunk array type %T in position delete file",
					iceberg.ErrInvalidSchema, arr)
			}
			if posArr.NullN() > 0 {
				return nil, fmt.Errorf("%w: null pos in position delete file",
					iceberg.ErrInvalidSchema)
			}
			for _, v := range posArr.Int64Values() {
				if v < 0 {
					return nil, fmt.Errorf("%w: negative pos %d in position delete file",
						iceberg.ErrInvalidSchema, v)
				}
				deletes[v] = struct{}{}
			}
		}
	}

	return deletes, nil
}

func releasePosDeletes(deletes map[string]*arrow.Chunked) {
	for _, chunk := range deletes {
		if chunk != nil {
			chunk.Release()
		}
	}
}

func collectPosDeleteBitmap(positionalDeletes positionDeletes) (*dv.RoaringPositionBitmap, error) {
	bitmap := dv.NewRoaringPositionBitmap()
	for _, chunk := range positionalDeletes {
		if chunk == nil {
			return nil, fmt.Errorf("%w: nil pos column chunk in position delete file",
				iceberg.ErrInvalidSchema)
		}
		if chunk.DataType().ID() != arrow.INT64 {
			return nil, fmt.Errorf("%w: unsupported pos column type %s in position delete file",
				iceberg.ErrInvalidSchema, chunk.DataType())
		}
		for _, arr := range chunk.Chunks() {
			posArr, ok := arr.(*array.Int64)
			if !ok {
				return nil, fmt.Errorf("%w: unsupported pos chunk array type %T in position delete file",
					iceberg.ErrInvalidSchema, arr)
			}
			if posArr.NullN() > 0 {
				return nil, fmt.Errorf("%w: null pos in position delete file",
					iceberg.ErrInvalidSchema)
			}
			for _, position := range posArr.Int64Values() {
				if position < 0 {
					return nil, fmt.Errorf("%w: negative pos %d in position delete file",
						iceberg.ErrInvalidSchema, position)
				}
				bitmap.Set(uint64(position))
			}
		}
	}

	return bitmap, nil
}

// combinePositionalDeletes builds the surviving-row index list for a single record
// batch. The deletes set holds file-relative (global) positions; cursor maps each
// batch row to its original file position (jumping across pruned row groups), and
// the appended indices are batch-local (the loop counter): they index into the
// batch handed to compute.Take, not into the whole file. Without batch-local
// indices, the second and later batches of a file would pass indices >= the batch
// length and compute.Take would fail with "index error: N out of bounds".
//
// A nil result means that no row in the batch was deleted. The index builder is
// allocated lazily after the first deleted row so clean batches can pass through
// without reconstructing their columns.
func combinePositionalDeletes(mem memory.Allocator, deletes set[int64], cursor *rowPositionCursor, nrows int64) arrow.Array {
	var bldr *array.Int64Builder

	for i := range nrows {
		if _, deleted := deletes[cursor.next()]; deleted {
			if bldr == nil {
				bldr = array.NewInt64Builder(mem)
				bldr.Reserve(int(i))
				for j := range i {
					bldr.Append(j)
				}
			}

			continue
		}

		if bldr != nil {
			bldr.Append(i)
		}
	}

	if bldr == nil {
		return nil
	}
	defer bldr.Release()

	return bldr.NewArray()
}

func combinePositionalDeleteBitmap(
	mem memory.Allocator,
	deletes *dv.RoaringPositionBitmap,
	cursor *rowPositionCursor,
	nrows int64,
) arrow.Array {
	// Unpruned batches occupy one contiguous file range. Query the bitmap once
	// rather than doing a roaring lookup for every row, including clean batches.
	// Advancing the cursor matters even when no rows are deleted.
	if !cursor.src.pruned() && cursor.consumed >= 0 && nrows >= 0 &&
		nrows <= math.MaxInt64-cursor.consumed {
		start := cursor.consumed
		cursor.consumed += nrows
		mask := deletes.KeepMaskForRange(uint64(start), uint64(cursor.consumed))
		if mask == nil {
			return nil
		}

		kept := 0
		for _, value := range mask {
			kept += bits.OnesCount8(value)
		}

		bldr := array.NewInt64Builder(mem)
		defer bldr.Release()
		if kept > 0 {
			bldr.Resize(kept)
		}
		if kept <= int(nrows)/2 {
			// For dense deletes, visit surviving bits instead of every row.
			// KeepMaskForRange clears padding bits in the final byte.
			for byteIndex, keep := range mask {
				for keep != 0 {
					bit := bits.TrailingZeros8(keep)
					bldr.UnsafeAppend(int64(byteIndex*8 + bit))
					keep &= keep - 1
				}
			}
		} else {
			// Dense keep masks are faster with a straight row scan.
			for i := range nrows {
				if mask[i>>3]&(1<<uint(i&7)) != 0 {
					bldr.UnsafeAppend(i)
				}
			}
		}

		return bldr.NewArray()
	}

	// Keep the row-by-row cursor for scans with skipped row groups.
	var bldr *array.Int64Builder

	for i := range nrows {
		if deletes.Contains(uint64(cursor.next())) {
			if bldr == nil {
				bldr = array.NewInt64Builder(mem)
				if i > 0 {
					// This row is deleted. Resize avoids rounding the maximum
					// survivor count up to a power of two as Reserve would.
					bldr.Resize(int(nrows - 1))
				}
				for j := range i {
					bldr.UnsafeAppend(j)
				}
			}

			continue
		}

		if bldr != nil {
			// A leading run of deletes needs no values buffer until a row
			// survives. Size it once for the remaining possible survivors.
			if bldr.Cap() == 0 {
				bldr.Resize(int(nrows - i))
			}
			bldr.UnsafeAppend(i)
		}
	}

	if bldr == nil {
		return nil
	}
	defer bldr.Release()

	return bldr.NewArray()
}

func processPositionalDeleteBitmap(
	ctx context.Context,
	deletes *dv.RoaringPositionBitmap,
	cursor *rowPositionCursor,
) recProcessFn {
	mem := compute.GetAllocator(ctx)

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		indices := combinePositionalDeleteBitmap(mem, deletes, cursor, r.NumRows())
		if indices == nil {
			r.Retain()

			return r, nil
		}
		defer indices.Release()

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(r), compute.NewDatumWithoutOwning(indices))
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

func processPositionalDeletes(ctx context.Context, deletes set[int64], cursor *rowPositionCursor) recProcessFn {
	mem := compute.GetAllocator(ctx)

	return func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		indices := combinePositionalDeletes(mem, deletes, cursor, r.NumRows())
		if indices == nil {
			r.Retain()

			return r, nil
		}
		defer indices.Release()

		out, err := compute.Take(ctx, *compute.DefaultTakeOptions(),
			compute.NewDatumWithoutOwning(r), compute.NewDatumWithoutOwning(indices))
		if err != nil {
			return nil, err
		}

		return out.(*compute.RecordDatum).Value, nil
	}
}

// enrichRecordsWithPosDeleteFields enriches a RecordBatch with the columns declared in the PositionalDeleteArrowSchema
// so that during the pipeline filtering stages that sheds filtered out records, we still have a way to
// preserve the original position of those records.
func enrichRecordsWithPosDeleteFields(ctx context.Context, filePath iceberg.DataFile, cursor *rowPositionCursor) recProcessFn {
	mem := compute.GetAllocator(ctx)

	return func(inData arrow.RecordBatch) (outData arrow.RecordBatch, err error) {
		defer inData.Release()

		schema := inData.Schema()
		fieldIdx := schema.NumFields()
		schema, err = schema.AddField(fieldIdx, PositionalDeleteArrowSchema.Field(0))
		if err != nil {
			return nil, err
		}
		schema, err = schema.AddField(fieldIdx+1, PositionalDeleteArrowSchema.Field(1))
		if err != nil {
			return nil, err
		}

		rb := array.NewRecordBuilder(mem, PositionalDeleteArrowSchema)
		defer rb.Release()

		filePathBldr, posBldr := rb.Field(0).(*array.StringBuilder), rb.Field(1).(*array.Int64Builder)

		for range inData.NumRows() {
			filePathBldr.Append(filePath.FilePath())
			posBldr.Append(cursor.next())
		}

		newCols := rb.NewRecordBatch()
		defer newCols.Release()

		columns := append(inData.Columns(), newCols.Column(0), newCols.Column(1))
		outData = array.NewRecordBatch(schema, columns, inData.NumRows())

		return outData, err
	}
}

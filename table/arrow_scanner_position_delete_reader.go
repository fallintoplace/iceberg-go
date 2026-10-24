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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"golang.org/x/sync/errgroup"
)

var PositionalDeleteArrowSchema, _ = SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, true, false)

type (
	positionDeletes      = []*arrow.Chunked
	perFilePosDeletes    = map[string]positionDeletes
	perDeleteFileTargets = map[string]map[string]struct{}
)

// releasePerFilePosDeletes releases every Arrow chunk in a positional-delete
// map. Required on every error return between readAllDeleteFiles and the
// iterator returned by createIterator — Arrow allocations are not freed by
// GC, so dropping the map on the floor leaks the chunks. Safe to call on a
// nil map; the nil-chunk guard is defensive — position-delete readers never insert a
// nil *arrow.Chunked, but the guard keeps callers safe if that invariant
// ever changes.
func releasePerFilePosDeletes(deletesPerFile perFilePosDeletes) {
	for _, chunks := range deletesPerFile {
		for _, chunk := range chunks {
			if chunk != nil {
				chunk.Release()
			}
		}
	}
}

// collectPositionDeleteFilesAndTargets indexes each positional-delete file and
// the data-file paths of tasks that reference it. A nil target set preserves
// the whole-file fallback when any referencing task lacks a usable data path.
func collectPositionDeleteFilesAndTargets(tasks []FileScanTask) (map[string]iceberg.DataFile, perDeleteFileTargets) {
	uniqueDeletes := make(map[string]iceberg.DataFile)
	targetsByDelete := make(perDeleteFileTargets)

	for _, task := range tasks {
		for _, deleteFile := range task.DeleteFiles {
			if deleteFile.ContentType() != iceberg.EntryContentPosDeletes {
				continue
			}

			deletePath := deleteFile.FilePath()
			if _, ok := uniqueDeletes[deletePath]; !ok {
				uniqueDeletes[deletePath] = deleteFile
			}

			targets, ok := targetsByDelete[deletePath]
			if !ok {
				targets = make(map[string]struct{})
				targetsByDelete[deletePath] = targets
			}
			if targets == nil {
				continue
			}
			if task.File == nil || task.File.FilePath() == "" {
				targetsByDelete[deletePath] = nil

				continue
			}
			targets[task.File.FilePath()] = struct{}{}
		}
	}

	return uniqueDeletes, targetsByDelete
}

// readAllDeleteFiles reads every referenced positional-delete file up front. It
// remains the path used by Transaction.makePositionDeleteRecordsForFilter and by
// the eager-vs-lazy benchmark; arrowScan.GetRecords uses lazyPositionDeleteLoader.
func readAllDeleteFiles(ctx context.Context, fs iceio.IO, tasks []FileScanTask, concurrency int) (perFilePosDeletes, error) {
	deletesPerFile := make(perFilePosDeletes)
	uniqueDeletes, targetsByDelete := collectPositionDeleteFilesAndTargets(tasks)

	if len(uniqueDeletes) == 0 {
		return deletesPerFile, nil
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	perFileChan := make(chan map[string]*arrow.Chunked, concurrency)
	go func() {
		// Inner g.Wait() gates the deferred channel close so a late worker
		// can't send on a closed channel. Outer g.Wait() below collects the
		// error — no cross-goroutine shared err variable.
		defer close(perFileChan)
		for _, v := range uniqueDeletes {
			g.Go(func() error {
				deletes, err := readDeletesForPaths(gctx, fs, v, targetsByDelete[v.FilePath()])
				if err != nil {
					return fmt.Errorf("read position deletes from %s: %w", v.FilePath(), err)
				}
				if deletes == nil {
					return nil
				}
				// Safe even after gctx cancellation: the channel buffer equals
				// g's concurrency limit, so every in-flight worker can send
				// without blocking while the for-range below drains to close.
				perFileChan <- deletes

				return nil
			})
		}
		_ = g.Wait()
	}()

	for deletes := range perFileChan {
		for file, arr := range deletes {
			deletesPerFile[file] = append(deletesPerFile[file], arr)
		}
	}

	if err := g.Wait(); err != nil {
		return deletesPerFile, err
	}

	return deletesPerFile, nil
}

// lazyPositionDeleteLoader indexes positional-delete metadata for a scan, but
// waits to open each delete file until a worker reaches a task that references
// it. Delete-file reads are cached by delete path, while the merged position
// bitmap is cached by target data file plus its delete-file set. Split tasks
// therefore share one immutable bitmap instead of rebuilding a hash set per
// task. Unions of multiple non-empty inputs are retained alongside the
// per-file bitmaps until release.
//
// The loader has the lifetime of exactly one scan. Each delete file and merged
// bitmap locks in its first result, including context errors, for every caller;
// a loader must not be reused for a retry with a different context.
type lazyPositionDeleteLoader struct {
	fs      iceio.IO
	files   map[string]*lazyPositionDeleteFile
	indexes []positionDeleteTaskIndex

	releaseOnce sync.Once
	released    atomic.Bool
}

var errPositionDeleteLoaderReleased = errors.New("position delete loader already released")

type lazyPositionDeleteFile struct {
	dataFile iceberg.DataFile
	targets  map[string]struct{}

	once    sync.Once
	bitmaps map[string]*dv.RoaringPositionBitmap
	err     error
}

type lazyPositionDeleteIndex struct {
	targetPath  string
	deletePaths []string

	once   sync.Once
	bitmap *dv.RoaringPositionBitmap
	err    error
}

type positionDeleteTaskIndex struct {
	targetPath string
	index      *lazyPositionDeleteIndex
}

func positionDeleteIndexKey(task FileScanTask) (key, targetPath string, deletePaths []string) {
	if task.File == nil {
		return "", "", nil
	}
	targetPath = task.File.FilePath()
	if targetPath == "" {
		return "", "", nil
	}
	// An applicable DV contains all earlier position deletes for this file.
	// Skip superseded files before loading or validating their contents.
	if len(task.DeletionVectorFiles) > 0 {
		return "", targetPath, nil
	}

	for _, deleteFile := range task.DeleteFiles {
		if deleteFile.ContentType() == iceberg.EntryContentPosDeletes {
			deletePaths = append(deletePaths, deleteFile.FilePath())
		}
	}
	if len(deletePaths) == 0 {
		return "", targetPath, nil
	}
	slices.Sort(deletePaths)
	deletePaths = slices.Compact(deletePaths)

	var builder strings.Builder
	writePart := func(part string) {
		builder.WriteString(strconv.Itoa(len(part)))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	writePart(targetPath)
	for _, path := range deletePaths {
		builder.WriteByte('|')
		writePart(path)
	}

	return builder.String(), targetPath, deletePaths
}

// sameOrderedPositionDeleteFiles compares only the references used to build a
// positional-delete index. Other delete types do not affect the index key.
func sameOrderedPositionDeleteFiles(left, right []iceberg.DataFile) bool {
	if len(left) != len(right) {
		return false
	}

	for i, file := range left {
		leftPosDelete := file.ContentType() == iceberg.EntryContentPosDeletes
		rightPosDelete := right[i].ContentType() == iceberg.EntryContentPosDeletes
		if leftPosDelete != rightPosDelete ||
			(leftPosDelete && file.FilePath() != right[i].FilePath()) {
			return false
		}
	}

	return true
}

func newLazyPositionDeleteLoader(fs iceio.IO, tasks []FileScanTask) *lazyPositionDeleteLoader {
	uniqueDeletes, targetsByDelete := collectPositionDeleteFilesAndTargets(tasks)
	if len(uniqueDeletes) == 0 {
		return nil
	}
	loader := &lazyPositionDeleteLoader{
		fs:      fs,
		files:   make(map[string]*lazyPositionDeleteFile, len(uniqueDeletes)),
		indexes: make([]positionDeleteTaskIndex, len(tasks)),
	}

	for path, deleteFile := range uniqueDeletes {
		loader.files[path] = &lazyPositionDeleteFile{
			dataFile: deleteFile,
			targets:  targetsByDelete[path],
		}
	}
	indexesByKey := make(map[string]*lazyPositionDeleteIndex)
	var previousIndex *lazyPositionDeleteIndex
	var previousTargetPath string
	for i, task := range tasks {
		// Consecutive splits of a data file usually share its delete set.
		// Avoid rebuilding and sorting the same long path key for each split.
		if i > 0 && previousIndex != nil && task.File != nil &&
			len(task.DeletionVectorFiles) == 0 &&
			task.File.FilePath() == previousTargetPath &&
			sameOrderedPositionDeleteFiles(task.DeleteFiles, tasks[i-1].DeleteFiles) {
			loader.indexes[i] = positionDeleteTaskIndex{
				targetPath: previousTargetPath,
				index:      previousIndex,
			}

			continue
		}

		key, targetPath, deletePaths := positionDeleteIndexKey(task)
		loader.indexes[i].targetPath = targetPath
		previousTargetPath = targetPath
		if key == "" {
			previousIndex = nil

			continue
		}
		index := indexesByKey[key]
		if index == nil {
			index = &lazyPositionDeleteIndex{
				targetPath:  targetPath,
				deletePaths: deletePaths,
			}
			indexesByKey[key] = index
		}
		loader.indexes[i].index = index
		previousIndex = index
	}

	return loader
}

func (l *lazyPositionDeleteLoader) loadDeleteFile(
	ctx context.Context, path string,
) (*lazyPositionDeleteFile, error) {
	cached, ok := l.files[path]
	if !ok {
		return nil, nil
	}

	cached.once.Do(func() {
		cached.bitmaps, cached.err = readPositionDeleteBitmapsForPaths(
			ctx, l.fs, cached.dataFile, cached.targets)
		if cached.err != nil {
			cached.bitmaps = nil
			cached.err = fmt.Errorf("read position deletes from %s: %w",
				cached.dataFile.FilePath(), cached.err)
		}
	})
	if cached.err != nil {
		return nil, cached.err
	}

	return cached, nil
}

func (l *lazyPositionDeleteLoader) buildIndex(
	ctx context.Context, targetPath string, deletePaths []string,
) (*dv.RoaringPositionBitmap, error) {
	var bitmap *dv.RoaringPositionBitmap
	owned := false
	for _, path := range deletePaths {
		cached, err := l.loadDeleteFile(ctx, path)
		if err != nil {
			return nil, err
		}
		if cached == nil {
			continue
		}

		deleteBitmap := cached.bitmaps[targetPath]
		if deleteBitmap == nil || deleteBitmap.IsEmpty() {
			continue
		}
		if bitmap == nil {
			bitmap = deleteBitmap

			continue
		}

		// Detach from the first cached bitmap once, then accumulate into the
		// private union. Cached inputs stay immutable for other split tasks.
		if !owned {
			merged := dv.NewRoaringPositionBitmap()
			merged.Or(bitmap)
			bitmap = merged
			owned = true
		}
		bitmap.Or(deleteBitmap)
	}

	return bitmap, nil
}

// load uses the ordinal in the task slice passed to newLazyPositionDeleteLoader.
// Workers preserve that ordinal even when they finish out of order, so this
// lookup does not rebuild or hash a key for every split. The target path rejects
// ordinals paired with another data file after reordering or filtering tasks,
// including ordinals that originally had no position deletes.
//
// The returned bitmap is shared and read-only. Callers must not mutate it with
// Set, Or, or RunLengthEncode; other split tasks may be reading the same bitmap.
func (l *lazyPositionDeleteLoader) load(ctx context.Context, taskIndex int, targetPath string) (*dv.RoaringPositionBitmap, error) {
	if l.released.Load() {
		return nil, errPositionDeleteLoaderReleased
	}
	if taskIndex < 0 || taskIndex >= len(l.indexes) {
		return nil, fmt.Errorf("%w: position delete task index %d out of range [0, %d)",
			iceberg.ErrInvalidArgument, taskIndex, len(l.indexes))
	}

	task := l.indexes[taskIndex]
	if task.targetPath != targetPath {
		return nil, fmt.Errorf("%w: position delete task index %d targets %q, got %q",
			iceberg.ErrInvalidArgument, taskIndex, task.targetPath, targetPath)
	}
	index := task.index
	if index == nil {
		return nil, nil
	}

	index.once.Do(func() {
		index.bitmap, index.err = l.buildIndex(ctx, index.targetPath, index.deletePaths)
	})
	if index.err != nil {
		return nil, index.err
	}

	return index.bitmap, nil
}

func (l *lazyPositionDeleteLoader) release() {
	if l == nil {
		return
	}

	l.releaseOnce.Do(func() {
		l.released.Store(true)
		for _, cached := range l.files {
			cached.bitmaps = nil
		}
		for _, task := range l.indexes {
			if task.index != nil {
				task.index.bitmap = nil
			}
		}
	})
}

func readPositionDeleteBitmapsForPaths(
	ctx context.Context,
	fs iceio.IO,
	dataFile iceberg.DataFile,
	targets map[string]struct{},
) (map[string]*dv.RoaringPositionBitmap, error) {
	deletes, err := readDeletesForPaths(ctx, fs, dataFile, targets)
	if err != nil {
		return nil, err
	}
	defer releasePosDeletes(deletes)

	bitmaps := make(map[string]*dv.RoaringPositionBitmap, len(deletes))
	for path, positions := range deletes {
		bitmap, err := collectPosDeleteBitmap(positionDeletes{positions})
		if err != nil {
			return nil, err
		}
		bitmaps[path] = bitmap
	}

	return bitmaps, nil
}

func readDeletes(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile) (map[string]*arrow.Chunked, error) {
	return readDeletesForPaths(ctx, fs, dataFile, nil)
}

func readDeletesForPaths(ctx context.Context, fs iceio.IO, dataFile iceberg.DataFile,
	targets map[string]struct{},
) (_ map[string]*arrow.Chunked, err error) {
	src, err := tblutils.GetFile(ctx, fs, dataFile, true)
	if err != nil {
		return nil, err
	}

	rdr, err := src.GetReader(ctx)
	if err != nil {
		return nil, err
	}
	defer iceinternal.CheckedClose(rdr, &err)

	schema, err := rdr.Schema()
	if err != nil {
		return nil, err
	}

	columns, err := positionDeleteProjectionIndices(schema, rdr)
	if err != nil {
		return nil, err
	}

	tester, err := newPositionDeleteRowGroupTester(schema, targets)
	if err != nil {
		return nil, err
	}

	records, err := rdr.GetRecords(ctx, columns, tester)
	if err != nil {
		return nil, err
	}
	// Do not unify dictionaries here: appendFilePathChunk decodes each batch to
	// string values, so independent dictionaries across batches are safe.
	defer records.Release()

	acc := newPosDeleteAccumulator(ctx, targets)
	defer func() {
		// Returning an error assigns the named return value before deferred
		// functions run, which releases builders on every error path.
		if err != nil {
			acc.release()
		}
	}()

	for records.Next() {
		if err := acc.appendRecord(ctx, records.RecordBatch()); err != nil {
			return nil, err
		}
	}
	if err := records.Err(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return acc.finish(), nil
}

func newPositionDeleteRowGroupTester(schema *arrow.Schema, targets map[string]struct{}) (*tblutils.ParquetRowGroupTester, error) {
	if len(targets) == 0 || len(targets) > inPredicateLimit {
		return nil, nil
	}
	pruningEnabled, err := positionDeletePruningEnabled(schema)
	if err != nil {
		return nil, err
	}
	if !pruningEnabled {
		return nil, nil
	}

	paths := make([]string, 0, len(targets))
	for path := range targets {
		paths = append(paths, path)
	}

	var filter iceberg.BooleanExpression
	if len(paths) == 1 {
		// A single target is the common case. EqualTo avoids building the
		// set literal used by IsIn and gives the stats/bloom planners the
		// simpler predicate directly.
		filter = iceberg.EqualTo(iceberg.Reference("file_path"), paths[0])
	} else {
		slices.Sort(paths)
		filter = iceberg.IsIn(iceberg.Reference("file_path"), paths...)
	}
	filter, err = iceberg.BindExpr(iceberg.PositionalDeleteSchema, filter, true)
	if err != nil {
		return nil, err
	}
	statsFieldIDs, err := iceberg.ExtractFieldIDs(filter)
	if err != nil {
		return nil, err
	}

	statsFn, err := newParquetRowGroupStatsEvaluator(iceberg.PositionalDeleteSchema, filter, false)
	if err != nil {
		return nil, err
	}
	bloomPreds, err := newBloomFilterPredicates(filter)
	if err != nil {
		return nil, err
	}

	return &tblutils.ParquetRowGroupTester{
		StatsFn:       statsFn,
		StatsFieldIDs: statsFieldIDs,
		BloomPreds:    bloomPreds,
	}, nil
}

func positionDeletePruningEnabled(schema *arrow.Schema) (bool, error) {
	// Row-group stats and Bloom predicates are keyed by Parquet physical field
	// IDs, while projection resolves these columns by their spec-defined names.
	// pqarrow carries the Parquet IDs into Arrow metadata, so only enable
	// pushdown when those two views agree for the reserved delete columns.
	physicalIDs := make(map[int]int)
	var collectIDs func([]arrow.Field)
	collectIDs = func(fields []arrow.Field) {
		for _, field := range fields {
			if id := getFieldID(field); id != nil {
				physicalIDs[*id]++
			}
			if nested, ok := field.Type.(arrow.NestedType); ok {
				collectIDs(nested.Fields())
			}
		}
	}
	collectIDs(schema.Fields())
	if len(physicalIDs) == 0 {
		// External position-delete files are allowed to omit Iceberg field IDs.
		// The name-based projection and row-level target filter remain safe, but
		// stats and Bloom pruning cannot be trusted without the IDs.
		return false, nil
	}

	deleteFields := iceberg.PositionalDeleteSchema.Fields()
	for _, field := range deleteFields {
		if physicalIDs[field.ID] > 1 {
			return false, fmt.Errorf("%w: position delete field ID %d is not unique",
				iceberg.ErrInvalidSchema, field.ID)
		}
	}

	pruningEnabled := true
	for _, want := range deleteFields {
		indices := schema.FieldIndices(want.Name)
		if len(indices) != 1 {
			return false, fmt.Errorf("%w: position delete file must contain exactly one %q column, found %d",
				iceberg.ErrInvalidSchema, want.Name, len(indices))
		}

		fieldID := getFieldID(schema.Field(indices[0]))
		if fieldID == nil {
			if physicalIDs[want.ID] != 0 {
				return false, fmt.Errorf("%w: position delete field ID %d is assigned to another column instead of %q",
					iceberg.ErrInvalidSchema, want.ID, want.Name)
			}
			// Missing IDs only disable pruning. Keep checking the remaining
			// columns so this fallback cannot hide an invalid ID mapping.
			pruningEnabled = false

			continue
		}
		if *fieldID != want.ID {
			if physicalIDs[want.ID] != 0 {
				return false, fmt.Errorf("%w: position delete column %q has field ID %d, want %d; field ID %d is assigned to another column",
					iceberg.ErrInvalidSchema, want.Name, *fieldID, want.ID, want.ID)
			}
			// A non-canonical ID is safe for name-based reading, but not for
			// stats or Bloom pruning. Keep the pre-pruning read compatible with
			// external writers that renumber fields.
			pruningEnabled = false
		}
	}

	return pruningEnabled, nil
}

func positionDeleteProjectionIndices(schema *arrow.Schema, reader tblutils.FileReader) ([]int, error) {
	filePathIndex, posIndex, err := positionDeleteColumnIndices(schema)
	if err != nil {
		return nil, err
	}
	fileMetadata, ok := reader.Metadata().(*metadata.FileMetaData)
	if !ok {
		return nil, fmt.Errorf("%w: position delete reader does not expose Parquet metadata", iceberg.ErrInvalidSchema)
	}

	// Parquet projection uses leaf indices. A nested row column can shift
	// those indices relative to the top-level Arrow fields.
	columns := make([]int, 2)
	for i, fieldIndex := range []int{filePathIndex, posIndex} {
		name := schema.Field(fieldIndex).Name
		columns[i] = fileMetadata.Schema.ColumnIndexByName(name)
		if columns[i] < 0 {
			return nil, fmt.Errorf("%w: position delete column %q must be a primitive column", iceberg.ErrInvalidSchema, name)
		}
	}

	return columns, nil
}

func positionDeleteColumnIndices(schema *arrow.Schema) (int, int, error) {
	// Position-delete columns are resolved by their spec-defined names because
	// Arrow schemas read from external files do not always retain Iceberg IDs.
	requiredColumn := func(name string) (int, error) {
		indices := schema.FieldIndices(name)
		if len(indices) != 1 {
			return 0, fmt.Errorf("%w: position delete file must contain exactly one %q column, found %d",
				iceberg.ErrInvalidSchema, name, len(indices))
		}

		return indices[0], nil
	}

	filePathIndex, err := requiredColumn("file_path")
	if err != nil {
		return 0, 0, err
	}
	posIndex, err := requiredColumn("pos")
	if err != nil {
		return 0, 0, err
	}

	return filePathIndex, posIndex, nil
}

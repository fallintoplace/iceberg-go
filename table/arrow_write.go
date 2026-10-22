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
	"iter"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const defaultBinPackLookback = 20

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

type arrowStatsCollector struct {
	fieldID          int
	schema           *iceberg.Schema
	defaultMode      tblutils.MetricsMode
	defaultModeError error
	columnModes      map[string]tblutils.MetricsMode
	columnModeErrors map[string]error
}

func (a *arrowStatsCollector) resolveColumnMetricsMode(colName string) tblutils.MetricsMode {
	if a.defaultModeError != nil {
		panic(a.defaultModeError)
	}

	if metMode, ok := a.columnModes[colName]; ok {
		return metMode
	}
	if err, ok := a.columnModeErrors[colName]; ok {
		panic(err)
	}

	return a.defaultMode
}

func (a *arrowStatsCollector) primitiveCollector(dt iceberg.PrimitiveType, isNested bool) (tblutils.StatisticsCollector, bool) {
	colName, ok := a.schema.FindColumnName(a.fieldID)
	if !ok {
		return tblutils.StatisticsCollector{}, false
	}

	metMode := a.resolveColumnMetricsMode(colName)

	switch dt.(type) {
	case iceberg.StringType:
	case iceberg.BinaryType:
	default:
		if metMode.Typ == tblutils.MetricModeTruncate {
			metMode = tblutils.MetricsMode{Typ: tblutils.MetricModeFull, Len: 0}
		}
	}

	if isNested && (metMode.Typ == tblutils.MetricModeTruncate || metMode.Typ == tblutils.MetricModeFull) {
		metMode = tblutils.MetricsMode{Typ: tblutils.MetricModeCounts}
	}

	return tblutils.StatisticsCollector{
		FieldID:    a.fieldID,
		IcebergTyp: dt,
		ColName:    colName,
		Mode:       metMode,
	}, true
}

func (a *arrowStatsCollector) variantCollector() (tblutils.StatisticsCollector, bool) {
	colName, ok := a.schema.FindColumnName(a.fieldID)
	if !ok {
		return tblutils.StatisticsCollector{}, false
	}

	return tblutils.StatisticsCollector{
		FieldID: a.fieldID,
		ColName: colName,
		Mode:    a.resolveColumnMetricsMode(colName),
	}, true
}

func statsPlanFieldCount(field iceberg.NestedField) int {
	switch typ := field.Type.(type) {
	case *iceberg.StructType:
		count := 0
		for _, nestedField := range typ.FieldList {
			count += statsPlanFieldCount(nestedField)
		}

		return count
	case *iceberg.ListType:
		return statsPlanFieldCount(typ.ElementField())
	case *iceberg.MapType:
		return statsPlanFieldCount(typ.KeyField()) + statsPlanFieldCount(typ.ValueField())
	default:
		return 1
	}
}

func collectStatsPlanField(visitor *arrowStatsCollector, result map[int]tblutils.StatisticsCollector, field iceberg.NestedField, isNested bool) {
	switch typ := field.Type.(type) {
	case *iceberg.StructType:
		for _, nestedField := range typ.FieldList {
			collectStatsPlanField(visitor, result, nestedField, true)
		}
	case *iceberg.ListType:
		collectStatsPlanField(visitor, result, typ.ElementField(), true)
	case *iceberg.MapType:
		collectStatsPlanField(visitor, result, typ.KeyField(), true)
		collectStatsPlanField(visitor, result, typ.ValueField(), true)
	case iceberg.VariantType:
		visitor.fieldID = field.ID
		if collector, ok := visitor.variantCollector(); ok {
			result[collector.FieldID] = collector
		}
	default:
		visitor.fieldID = field.ID
		collector, ok := visitor.primitiveCollector(field.Type.(iceberg.PrimitiveType), isNested)
		if ok {
			result[collector.FieldID] = collector
		}
	}
}

func computeStatsPlan(sc *iceberg.Schema, props iceberg.Properties) (result map[int]tblutils.StatisticsCollector, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("%w: %s", iceberg.ErrInvalidSchema, e)
			case error:
				err = fmt.Errorf("error encountered during schema visitor: %w", e)
			}
		}
	}()

	if sc == nil {
		return nil, fmt.Errorf("%w: cannot visit nil schema", iceberg.ErrInvalidArgument)
	}

	defaultMode, defaultModeErr := tblutils.MatchMetricsMode(
		props.Get(DefaultWriteMetricsModeKey, DefaultWriteMetricsModeDefault))
	overrideCount := 0
	for key := range props {
		if strings.HasPrefix(key, MetricsModeColumnConfPrefix+".") {
			overrideCount++
		}
	}

	var columnModes map[string]tblutils.MetricsMode
	var columnModeErrors map[string]error
	for key, rawMode := range props {
		colName, ok := strings.CutPrefix(key, MetricsModeColumnConfPrefix+".")
		if !ok {
			continue
		}

		mode, err := tblutils.MatchMetricsMode(rawMode)
		if err != nil {
			if columnModeErrors == nil {
				columnModeErrors = make(map[string]error, overrideCount)
			}

			columnModeErrors[colName] = err

			continue
		}
		if columnModes == nil {
			columnModes = make(map[string]tblutils.MetricsMode, overrideCount)
		}
		columnModes[colName] = mode
	}

	visitor := &arrowStatsCollector{
		schema:           sc,
		defaultMode:      defaultMode,
		defaultModeError: defaultModeErr,
		columnModes:      columnModes,
		columnModeErrors: columnModeErrors,
	}

	fields := sc.FieldsRef(internal.SchemaRef{})
	resultCount := 0
	for _, field := range fields {
		resultCount += statsPlanFieldCount(field)
	}

	result = make(map[int]tblutils.StatisticsCollector, resultCount)
	for _, field := range fields {
		collectStatsPlanField(visitor, result, field, strings.Contains(field.Name, "."))
	}

	return result, nil
}

func filesToDataFiles(ctx context.Context, fileIO iceio.IO, meta *MetadataBuilder, filePaths []string, concurrency int) (_ []iceberg.DataFile, err error) {
	partitionSpec, err := meta.CurrentSpec()
	if err != nil || partitionSpec == nil {
		return nil, fmt.Errorf("%w: cannot add files without a current spec", err)
	}

	if err := checkNoUnknownTransform(partitionSpec); err != nil {
		return nil, err
	}

	currentSchema, currentSpec := meta.CurrentSchema(), *partitionSpec

	dataFiles := make([]iceberg.DataFile, len(filePaths))
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(concurrency)
	for i, filePath := range filePaths {
		eg.Go(func() (err error) {
			defer func() {
				if r := recover(); r != nil {
					switch e := r.(type) {
					case error:
						err = fmt.Errorf("error encountered during file conversion: %w", e)
					default:
						err = fmt.Errorf("error encountered during file conversion: %v", e)
					}
				}
			}()

			// Pass 0 so AddFiles does not claim sort_order_id on files
			// it did not write (#1184). Callers that know the file's
			// sort layout use FileToDataFile with an explicit id.
			dataFiles[i] = fileToDataFile(ctx, fileIO, filePath, currentSchema, currentSpec, 0, meta.props)

			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return dataFiles, nil
}

// FileToDataFile builds a DataFile for an existing parquet file at
// filePath, reading its footer to populate record count, file size,
// column sizes, value/null counts and lower/upper bounds, and to infer
// partition values for order-preserving transforms. sortOrderID is
// stamped onto the DataFile so callers converting foreign parquet
// footers can convey the file's sort layout (spec data-file field
// sort_order_id); pass 0 to make no claim. Panics from the unexported
// conversion are recovered into an error.
func FileToDataFile(ctx context.Context, fileIO iceio.IO, filePath string, currentSchema *iceberg.Schema, currentSpec iceberg.PartitionSpec, sortOrderID int, props iceberg.Properties) (df iceberg.DataFile, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case error:
				err = fmt.Errorf("error encountered during file conversion: %w", e)
			default:
				err = fmt.Errorf("error encountered during file conversion: %v", e)
			}
		}
	}()

	return fileToDataFile(ctx, fileIO, filePath, currentSchema, currentSpec, sortOrderID, props), nil
}

func fileToDataFile(ctx context.Context, fileIO iceio.IO, filePath string, currentSchema *iceberg.Schema, currentSpec iceberg.PartitionSpec, sortOrderID int, props iceberg.Properties) iceberg.DataFile {
	format := tblutils.FormatFromFileName(filePath)
	rdr := must(format.Open(ctx, fileIO, filePath))
	defer rdr.Close()

	arrSchema := must(rdr.Schema())
	if err := checkArrowSchemaCompatWithProperties(currentSchema, arrSchema, false, props); err != nil {
		panic(err)
	}

	pathToIDSchema := currentSchema
	if fileHasIDs := must(VisitArrowSchema(arrSchema, hasIDs{})); fileHasIDs {
		pathToIDSchema = must(ArrowSchemaToIcebergWithOptions(arrSchema, ArrowToIcebergOptions{
			TableSchema:     currentSchema,
			TableProperties: props,
		}))
	}
	statistics := format.DataFileStatsFromMeta(
		rdr.Metadata(),
		must(computeStatsPlan(currentSchema, props)),
		must(format.PathToIDMapping(pathToIDSchema)),
		tblutils.VariantFieldIDsFromSchema(currentSchema),
		arrSchema,
	)

	partitionValues := make(map[int]any)
	if !currentSpec.Equals(*iceberg.UnpartitionedSpec) {
		for _, field := range currentSpec.Fields() {
			if !field.Transform.PreservesOrder() {
				panic(fmt.Errorf("cannot infer partition value from parquet metadata for a non-linear partition field: %s with transform %s", field.Name, field.Transform))
			}

			partitionVal := statistics.PartitionValue(field, currentSchema)
			if partitionVal != nil {
				partitionValues[field.FieldID] = partitionVal
			}
		}
	}

	return statistics.ToDataFile(tblutils.DataFileOpts{
		Schema:          currentSchema,
		Spec:            currentSpec,
		Path:            filePath,
		Format:          iceberg.ParquetFile,
		Content:         iceberg.EntryContentData,
		FileSize:        rdr.SourceFileSize(),
		PartitionValues: partitionValues,
		SortOrderID:     sortOrderID,
	})
}

func recordNBytes(rec arrow.RecordBatch) (total int64) {
	for _, c := range rec.Columns() {
		total += int64(c.Data().SizeInBytes())
	}

	return total
}

func binPackRecords(itr iter.Seq2[arrow.RecordBatch, error], recordLookback int, targetFileSize int64) iter.Seq[[]arrow.RecordBatch] {
	return internal.PackingIterator(func(yield func(arrow.RecordBatch) bool) {
		for rec, err := range itr {
			if err != nil {
				panic(err)
			}

			rec.Retain()
			if !yield(rec) {
				return
			}
		}
	}, targetFileSize, recordLookback, recordNBytes, false)
}

type recordWritingArgs struct {
	sc              *arrow.Schema
	itr             iter.Seq2[arrow.RecordBatch, error]
	fs              iceio.WriteFileIO
	writeUUID       *uuid.UUID
	counter         iter.Seq[int]
	maxWriteWorkers int
	clustered       bool
	factoryOpts     []writerFactoryOption

	// recordBatchBufferSize overrides the rolling writers' record
	// channel capacity; non-positive uses the default (see
	// rollingDataWriterQueueCapacity).
	recordBatchBufferSize int
	// existingDVs maps a data file path to the positions already recorded in
	// its current deletion vector. On the v3 DV write path these are folded
	// into the newly written DV so a data file that already had a DV ends up
	// with a single merged vector (the caller removes the superseded one).
	existingDVs map[string]*dv.RoaringPositionBitmap
}

// checkNoUnknownTransform rejects writes against a spec containing an unknown
// partition transform. We can't compute partition values for it, so writing
// would produce null values under a field=<transform-name> dir that Java and
// PyIceberg can't read. The spec forbids committing against such a spec.
func checkNoUnknownTransform(spec *iceberg.PartitionSpec) error {
	if spec == nil {
		return nil
	}
	for _, f := range spec.Fields() {
		if _, ok := f.Transform.(iceberg.UnknownTransform); ok {
			return fmt.Errorf("%w: cannot write to a partition spec with unknown transform: %s", iceberg.ErrInvalidTransform, f.Transform)
		}
	}

	return nil
}

// checkNoUnknownTransformInSpecs applies checkNoUnknownTransform to every spec
// the given data files were written under. Delete writes target existing files,
// so the relevant spec is each file's own, which may be an older one.
func checkNoUnknownTransformInSpecs(meta Metadata, partitionContextByFilePath map[string]partitionContext) error {
	seen := make(map[int32]struct{}, len(partitionContextByFilePath))
	for _, pCtx := range partitionContextByFilePath {
		if _, ok := seen[pCtx.specID]; ok {
			continue
		}
		seen[pCtx.specID] = struct{}{}
		if err := checkNoUnknownTransform(meta.PartitionSpecByID(int(pCtx.specID))); err != nil {
			return err
		}
	}

	currentSpec := meta.PartitionSpec()

	return checkNoUnknownTransform(&currentSpec)
}

func recordsToDataFiles(ctx context.Context, rootLocation string, meta *MetadataBuilder, args recordWritingArgs) (ret iter.Seq2[iceberg.DataFile, error]) {
	spec, err := meta.CurrentSpec()
	if err == nil {
		err = checkNoUnknownTransform(spec)
	}
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	if args.counter == nil {
		args.counter = internal.Counter(0)
	}

	defer func() {
		if r := recover(); r != nil {
			var err error
			switch e := r.(type) {
			case error:
				err = fmt.Errorf("error encountered during file writing: %w", e)
			default:
				err = fmt.Errorf("error encountered during position delete file writing: %v", e)
			}
			ret = func(yield func(iceberg.DataFile, error) bool) {
				yield(nil, err)
			}
		}
	}()

	if args.writeUUID == nil {
		u := uuid.Must(uuid.NewRandom())
		args.writeUUID = &u
	}

	targetFileSize := int64(meta.props.GetInt(WriteTargetFileSizeBytesKey,
		WriteTargetFileSizeBytesDefault))

	nameMapping := meta.CurrentSchema().NameMapping()
	taskSchema, err := ArrowSchemaToIcebergWithOptions(args.sc, ArrowToIcebergOptions{
		NameMapping:     nameMapping,
		TableSchema:     meta.CurrentSchema(),
		TableProperties: meta.props,
	})
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	factory, err := newWriterFactory(rootLocation, args, meta, taskSchema, targetFileSize, args.factoryOpts...)
	if err != nil {
		panic(err)
	}

	if factory.currentSpec.IsUnpartitioned() {
		return unpartitionedWrite(ctx, factory, args.itr)
	}

	if args.clustered {
		return clusteredPartitionedWrite(ctx, factory.currentSpec, meta.CurrentSchema(), factory, args.itr)
	}

	partitionWriter := newPartitionedFanoutWriter(factory.currentSpec, meta.CurrentSchema(), args.itr, factory)
	workers := config.EnvConfig.MaxWorkers
	if args.maxWriteWorkers > 0 {
		workers = args.maxWriteWorkers
	}

	return partitionWriter.Write(ctx, workers)
}

func unpartitionedWrite(ctx context.Context, factory *writerFactory, records iter.Seq2[arrow.RecordBatch, error]) iter.Seq2[iceberg.DataFile, error] {
	outputCh := make(chan iceberg.DataFile, 1)
	errCh := make(chan error, 1)
	writerCtx, cancel := context.WithCancel(ctx)

	go func() {
		defer close(outputCh)
		defer factory.stopCount()

		writer := factory.newRollingDataWriter(writerCtx, "", nil, outputCh)
		for rec, err := range records {
			if err != nil {
				errCh <- err
				close(errCh)
				writer.abortAndWait()

				return
			}

			if err := writer.Add(rec); err != nil {
				errCh <- err
				close(errCh)
				writer.abortAndWait()

				return
			}
		}
		if err := writer.closeAndWait(); err != nil {
			errCh <- err
		}
		close(errCh)
	}()

	return func(yield func(iceberg.DataFile, error) bool) {
		defer func() {
			for range outputCh {
			}
		}()
		defer cancel()
		for df := range outputCh {
			if !yield(df, nil) {
				return
			}
		}
		if err := <-errCh; err != nil {
			yield(nil, err)
		}
	}
}

type partitionContext struct {
	partitionData map[int]any
	specID        int32
}

func positionDeleteRecordsToDataFiles(ctx context.Context, rootLocation string, meta *MetadataBuilder, partitionContextByFilePath map[string]partitionContext, args recordWritingArgs) (ret iter.Seq2[iceberg.DataFile, error]) {
	if args.counter == nil {
		args.counter = internal.Counter(0)
	}

	defer func() {
		if r := recover(); r != nil {
			var err error
			switch e := r.(type) {
			case error:
				err = fmt.Errorf("error encountered during position delete file writing: %w", e)
			default:
				err = fmt.Errorf("error encountered during position delete file writing: %v", e)
			}
			ret = func(yield func(iceberg.DataFile, error) bool) {
				yield(nil, err)
			}
		}
	}()

	latestMetadata, err := meta.Build()
	if err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	// Check the specs the targeted data files were written under, not just the
	// current one: a table that has since evolved past the unknown transform
	// still reaches this path through its historic specs.
	if err := checkNoUnknownTransformInSpecs(latestMetadata, partitionContextByFilePath); err != nil {
		return func(yield func(iceberg.DataFile, error) bool) {
			yield(nil, err)
		}
	}

	// V3+ tables write deletion vectors via Puffin regardless of partitioning,
	// matching the Java reference implementation, which hardcodes DV for v3 and
	// does not expose a property to opt out. The DV path threads each data
	// file's spec id and partition record through DVWriter so partitioned
	// outputs carry the correct manifest-entry partition.
	if latestMetadata.Version() >= 3 {
		return positionDeleteRecordsToDataFilesDV(ctx, rootLocation, args, latestMetadata, partitionContextByFilePath)
	}

	if args.writeUUID == nil {
		u := uuid.Must(uuid.NewRandom())
		args.writeUUID = &u
	}

	targetFileSize := int64(meta.props.GetInt(WriteTargetFileSizeBytesKey,
		WriteTargetFileSizeBytesDefault))

	if latestMetadata.PartitionSpec().IsUnpartitioned() {
		cw := newConcurrentDataFileWriter(func(rootLocation string, fs iceio.WriteFileIO, meta *MetadataBuilder, props iceberg.Properties, opts ...dataFileWriterOption) (dataFileWriter, error) {
			return newPositionDeleteWriter(rootLocation, fs, meta, props, opts...)
		}, withSchemaSanitization(false))
		nextCount, stopCount := iter.Pull(args.counter)
		tasks := func(yield func(WriteTask) bool) {
			defer stopCount()

			fileCount := 0
			for batch := range binPackRecords(args.itr, defaultBinPackLookback, targetFileSize) {
				cnt, _ := nextCount()
				fileCount++
				t := WriteTask{
					Uuid:        *args.writeUUID,
					ID:          cnt,
					PartitionID: iceberg.UnpartitionedSpec.ID(),
					FileCount:   fileCount,
					Schema:      iceberg.PositionalDeleteSchema,
					Batches:     batch,
				}
				if !yield(t) {
					return
				}
			}
		}

		return cw.writeFiles(ctx, rootLocation, args.fs, meta, meta.props, nil, tasks)
	}
	factory, err := newWriterFactory(rootLocation, args, meta, iceberg.PositionalDeleteSchema, targetFileSize,
		withContentType(iceberg.EntryContentPosDeletes),
		withFactoryFileSchema(iceberg.PositionalDeleteSchema))
	if err != nil {
		panic(err)
	}

	partitionWriter := newPositionDeletePartitionedFanoutWriter(latestMetadata, partitionContextByFilePath, args.itr, factory)
	workers := config.EnvConfig.MaxWorkers

	return partitionWriter.Write(ctx, workers)
}

// positionDeleteRecordsToDataFilesDV produces deletion-vector Puffin output
// for v3+ tables. Each row's data file path must have an entry in
// partitionContextByFilePath: the (specID, partitionData) is captured on
// DVWriter.Load/AddPosition and lands on the output DataFile manifest entry, mirroring
// Java's BaseDVFileWriter, which keys deletes by (path, spec, partition).
// A missing entry is a programming error (the caller failed to pre-register
// every data file targeted by the delete) and surfaces as an error rather
// than silently producing a default-partition entry.
func positionDeleteRecordsToDataFilesDV(ctx context.Context, rootLocation string, args recordWritingArgs, metadata Metadata, partitionContextByFilePath map[string]partitionContext) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		writer := dv.NewDVWriter(args.fs, func(id int32) *iceberg.PartitionSpec {
			return metadata.PartitionSpecByID(int(id))
		})

		// Seed the writer with any deletion vector the referenced data file
		// already carries so its previously deleted positions survive into the
		// merged DV. The scan that produced args.itr does NOT apply the existing
		// DVs (makePositionDeleteRecordsForFilter builds its FileScanTasks
		// without DeleteFiles), so the incoming batches may re-report positions
		// already in the old DV; re-Setting them on top of the seed is a no-op
		// because the bitmap is idempotent. The seed is what guarantees the old
		// positions survive — do not remove it, or a second delete would drop
		// the earlier deletes and resurrect those rows. The spec allows at most
		// one DV per data file, so the caller supersedes the old DV once this
		// merged one is written.
		hasEntries := false
		for filePath, bitmap := range args.existingDVs {
			pCtx, ok := partitionContextByFilePath[filePath]
			if !ok {
				yield(nil, fmt.Errorf("unexpected missing partition context for existing deletion vector path %s", filePath))

				return
			}
			writer.Load(filePath, bitmap, pCtx.specID, pCtx.partitionData)
			hasEntries = true
		}

		for batch, err := range args.itr {
			if err != nil {
				yield(nil, err)

				return
			}

			filePaths := batch.Column(0).(*array.String)
			positions := batch.Column(1).(*array.Int64)

			for i := range batch.NumRows() {
				filePath := filePaths.Value(int(i))
				pCtx, ok := partitionContextByFilePath[filePath]
				if !ok {
					yield(nil, fmt.Errorf("unexpected missing partition context for path %s", filePath))

					return
				}
				if err := writer.AddPosition(filePath, positions.Value(int(i)), pCtx.specID, pCtx.partitionData); err != nil {
					yield(nil, err)

					return
				}
				hasEntries = true
			}
		}

		if !hasEntries {
			return
		}

		if args.writeUUID == nil {
			u := uuid.Must(uuid.NewRandom())
			args.writeUUID = &u
		}
		location := rootLocation + "/data/" + fmt.Sprintf("00000-0-%s-deletes.puffin", *args.writeUUID)

		dataFiles, err := writer.Flush(ctx, location)
		if err != nil {
			yield(nil, err)

			return
		}

		for _, df := range dataFiles {
			if !yield(df, nil) {
				return
			}
		}
	}
}

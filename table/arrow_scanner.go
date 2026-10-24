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
	"io"
	"iter"
	"maps"
	"slices"
	"strconv"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/compute/exprs"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/dv"
	tblutils "github.com/apache/iceberg-go/table/internal"
	"github.com/apache/iceberg-go/table/substrait"
	"github.com/substrait-io/substrait-go/v8/expr"
)

const (
	ScanOptionArrowUseLargeTypes = "arrow.use_large_types"
	ScanOptionRowLineageEnabled  = "row_lineage.enabled"
)

type set[T comparable] map[T]struct{}

type recProcessFn func(arrow.RecordBatch) (arrow.RecordBatch, error)

func filterRecords(ctx context.Context, recordFilter expr.Expression) recProcessFn {
	return func(rec arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer rec.Release()

		input := compute.NewDatumWithoutOwning(rec)
		mask, err := exprs.ExecuteScalarExpression(ctx, rec.Schema(), recordFilter, input)
		if err != nil {
			return nil, err
		}
		defer mask.Release()

		result, err := compute.Filter(ctx, input, mask, *compute.DefaultFilterOptions())
		if err != nil {
			return nil, err
		}

		return result.(*compute.RecordDatum).Value, nil
	}
}

type arrowScan struct {
	fs              iceio.IO
	metadata        Metadata
	scanSchema      *iceberg.Schema
	projectedSchema *iceberg.Schema
	boundRowFilter  iceberg.BooleanExpression
	// rowGroupFilter is used only for Parquet statistics and bloom-filter
	// pruning. It lets callers keep boundRowFilter as AlwaysTrue while they
	// must evaluate the real row filter after position-dependent enrichment.
	rowGroupFilter    iceberg.BooleanExpression
	filterSchema      *iceberg.Schema
	caseSensitive     bool
	rowLimit          int64
	options           iceberg.Properties
	filterPlanCache   compiledFileFilterPlanCache
	fileReadPlanCache preparedFileReadPlanCache
	cacheFileReadPlan bool
	// taskResidualsBound means every non-nil residual passed to GetRecords was
	// bound and validated against filterSchema, so rowFilterForTask can skip binding.
	taskResidualsBound bool

	useLargeTypes bool
	concurrency   int

	// arrowBatchSize, when positive, overrides the table's
	// read.parquet.batch-size property for this scan's reads.
	arrowBatchSize int
}

// preparedFileRead contains the physical schema projection shared by all
// tasks reading the same data file during one scan. The actual FileReader is
// intentionally not shared: split tasks may run concurrently and each reader
// owns its column readers and input handle.
type preparedFileRead struct {
	schema     *iceberg.Schema
	colIndices []int
}

type preparedFileReadEntry struct {
	once sync.Once
	plan *preparedFileRead
	err  error
}

type preparedFileReadPlanCache struct {
	mu      sync.Mutex
	entries map[string]*preparedFileReadEntry
}

func (c *preparedFileReadPlanCache) entry(path string) *preparedFileReadEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries == nil {
		c.entries = make(map[string]*preparedFileReadEntry)
	}
	if entry, ok := c.entries[path]; ok {
		return entry
	}

	entry := &preparedFileReadEntry{}
	c.entries[path] = entry

	return entry
}

// arrowScanInvariants holds metadata-derived values that cannot change during
// a scan. The snapshots returned by Metadata are owned by the scan and are
// safe to share read-only across file workers.
type arrowScanInvariants struct {
	tableSchema       *iceberg.Schema
	tableProperties   iceberg.Properties
	projectedIDs      set[int]
	nameMapping       iceberg.NameMapping
	outputArrowSchema *arrow.Schema
}

// collectLeafIDs recursively collects leaf field IDs from a type
func collectLeafIDs(typ iceberg.Type, fieldID int, idset set[int]) {
	switch t := typ.(type) {
	case *iceberg.MapType:
		// For maps, collect leaf IDs from both key and value
		collectLeafIDs(t.KeyType, t.KeyID, idset)
		collectLeafIDs(t.ValueType, t.ValueID, idset)
	case *iceberg.ListType:
		// For lists, collect leaf IDs from the element
		collectLeafIDs(t.Element, t.ElementID, idset)
	case *iceberg.StructType:
		// For structs, collect leaf IDs from all fields
		for _, field := range t.FieldList {
			collectLeafIDs(field.Type, field.ID, idset)
		}
	default:
		// Primitive type - this is a leaf
		idset[fieldID] = struct{}{}
	}
}

func addFilterFieldIDs(idset set[int], rowFilter iceberg.BooleanExpression) error {
	if rowFilter == nil {
		return nil
	}

	extracted, err := iceberg.ExtractFieldIDs(rowFilter)
	if err != nil {
		return err
	}

	for _, id := range extracted {
		idset[id] = struct{}{}
	}

	return nil
}

func (as *arrowScan) projectedFieldIDs(rowFilter iceberg.BooleanExpression, equalityDeletes []*equalityDeleteSet) (set[int], error) {
	idset := set[int]{}
	// Collect leaf field IDs for column pruning.
	// For nested types (map, list, struct), we recursively descend to find
	// the actual leaf primitive fields, not the intermediate container nodes.
	for _, field := range as.projectedSchema.Fields() {
		collectLeafIDs(field.Type, field.ID, idset)
	}

	if err := addFilterFieldIDs(idset, rowFilter); err != nil {
		return nil, err
	}
	for _, deletes := range equalityDeletes {
		for _, id := range deletes.fieldIDs {
			idset[id] = struct{}{}
		}
	}

	return idset, nil
}

func (as *arrowScan) scanInvariants(tableProperties iceberg.Properties) (*arrowScanInvariants, error) {
	// Filter columns are added per task below. A local task may have a
	// partition-elided residual that no longer references the original filter
	// fields.
	projectedIDs, err := as.projectedFieldIDs(nil, nil)
	if err != nil {
		return nil, err
	}

	return &arrowScanInvariants{
		tableSchema:     as.metadata.CurrentSchema(),
		tableProperties: tableProperties,
		projectedIDs:    projectedIDs,
		nameMapping:     as.metadata.NameMapping(),
	}, nil
}

func (as *arrowScan) addTaskProjectedFieldIDs(invariants *arrowScanInvariants, tasks []FileScanTask) error {
	// Tasks without a residual still use the scan's original filter. Add those
	// fields once, then add only the actual residual fields for other tasks.
	for _, task := range tasks {
		if task.Residual == nil {
			if err := addFilterFieldIDs(invariants.projectedIDs, as.boundRowFilter); err != nil {
				return err
			}

			break
		}
	}

	for _, task := range tasks {
		if task.Residual == nil {
			continue
		}

		rowFilter, err := as.rowFilterForTask(task)
		if err != nil {
			return err
		}

		if err := addFilterFieldIDs(invariants.projectedIDs, rowFilter); err != nil {
			return err
		}
	}

	return nil
}

const maxInFlightTasksPerWorker = 1

type taskCredits chan struct{}

func newTaskCredits() taskCredits {
	return make(taskCredits, maxInFlightTasksPerWorker)
}

func (c taskCredits) acquire(ctx context.Context) error {
	select {
	case c <- struct{}{}:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (c taskCredits) release() {
	if c != nil {
		<-c
	}
}

type enumeratedRecord struct {
	Record  tblutils.Enumerated[arrow.RecordBatch]
	Task    tblutils.Enumerated[FileScanTask]
	Err     error
	credits taskCredits
}

type recordSink struct {
	out     chan<- enumeratedRecord
	credits taskCredits
	held    bool
}

func newRecordSink(out chan<- enumeratedRecord) *recordSink {
	return &recordSink{out: out, credits: newTaskCredits()}
}

func (s *recordSink) reserve(ctx context.Context) error {
	if err := s.credits.acquire(ctx); err != nil {
		return err
	}
	s.held = true

	return nil
}

func (s *recordSink) send(rec enumeratedRecord) {
	if rec.Record.Last {
		rec.credits = s.handOff()
	}
	s.out <- rec
}

func (s *recordSink) fail(task tblutils.Enumerated[FileScanTask], err error) {
	s.out <- enumeratedRecord{Task: task, Err: err, credits: s.handOff()}
}

func (s *recordSink) handOff() taskCredits {
	if !s.held {
		return nil
	}
	s.held = false

	return s.credits
}

func (as *arrowScan) prepareToRead(ctx context.Context, file iceberg.DataFile, invariants *arrowScanInvariants) (iceSchema *iceberg.Schema, colIndices []int, rdr tblutils.FileReader, err error) {
	src, err := tblutils.GetFile(ctx, as.fs, file, false)
	if err != nil {
		return nil, nil, nil, err
	}

	rdr, err = src.GetReader(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	keepReader := false
	defer func(reader tblutils.FileReader) {
		if !keepReader {
			_ = reader.Close()
		}
	}(rdr)

	if !as.cacheFileReadPlan {
		var fileSchema *arrow.Schema
		fileSchema, colIndices, err = rdr.PrunedSchema(
			invariants.projectedIDs, invariants.nameMapping)
		if err != nil {
			return nil, nil, nil, err
		}

		iceSchema, err = ArrowSchemaToIcebergWithOptions(fileSchema, ArrowToIcebergOptions{
			NameMapping:     invariants.nameMapping,
			TableSchema:     invariants.tableSchema,
			TableProperties: invariants.tableProperties,
		})
		if err != nil {
			return nil, nil, nil, err
		}

		keepReader = true

		return iceSchema, colIndices, rdr, nil
	}

	entry := as.fileReadPlanCache.entry(file.FilePath())
	entry.once.Do(func() {
		var fileSchema *arrow.Schema
		var colIndices []int
		fileSchema, colIndices, entry.err = rdr.PrunedSchema(
			invariants.projectedIDs, invariants.nameMapping)
		if entry.err != nil {
			return
		}

		var iceSchema *iceberg.Schema
		iceSchema, entry.err = ArrowSchemaToIcebergWithOptions(fileSchema, ArrowToIcebergOptions{
			NameMapping:     invariants.nameMapping,
			TableSchema:     invariants.tableSchema,
			TableProperties: invariants.tableProperties,
		})
		if entry.err == nil {
			entry.plan = &preparedFileRead{
				schema:     iceSchema,
				colIndices: colIndices,
			}
		}
	})
	if entry.err != nil {
		return nil, nil, nil, entry.err
	}

	keepReader = true

	return entry.plan.schema, entry.plan.colIndices, rdr, nil
}

func (as *arrowScan) getRecordFilter(ctx context.Context, fileSchema *iceberg.Schema, rowFilter iceberg.BooleanExpression) (recProcessFn, bool, error) {
	if rowFilter == nil || rowFilter.Equals(iceberg.AlwaysTrue{}) {
		return nil, false, nil
	}

	plan, err := compileFileFilterPlan(fileSchema, rowFilter, as.caseSensitive, true, false)
	if err != nil {
		return nil, false, err
	}

	return as.recordProcessorWithExtracts(ctx, plan), plan.dropFile, nil
}

// recordProcessorWithExtracts returns the plan's record filter, wrapped with the
// per-batch variant extract residual when the plan carries extract terms.
func (as *arrowScan) recordProcessorWithExtracts(ctx context.Context, plan *compiledFileFilterPlan) recProcessFn {
	base := plan.recordProcessor(ctx)
	if base == nil {
		// No record filter (nil plan or filter folded to AlwaysTrue): keep every row, no residual.
		return nil
	}
	if len(plan.extracts) == 0 {
		return base
	}

	return as.extractResidualFilter(ctx, plan.extracts, base)
}

func (as *arrowScan) getRecordFilterFromPlan(ctx context.Context, plan *compiledFileFilterPlan) (recProcessFn, bool, error) {
	if plan == nil {
		return nil, false, nil
	}

	return as.recordProcessorWithExtracts(ctx, plan), plan.dropFile, nil
}

type filterBindingState struct {
	hasBound   bool
	hasUnbound bool
}

type filterBindingVisitor struct{}

func (filterBindingVisitor) VisitTrue() filterBindingState  { return filterBindingState{} }
func (filterBindingVisitor) VisitFalse() filterBindingState { return filterBindingState{} }
func (filterBindingVisitor) VisitNot(child filterBindingState) filterBindingState {
	return child
}

func (filterBindingVisitor) VisitAnd(left, right filterBindingState) filterBindingState {
	return filterBindingState{
		hasBound:   left.hasBound || right.hasBound,
		hasUnbound: left.hasUnbound || right.hasUnbound,
	}
}

func (filterBindingVisitor) VisitOr(left, right filterBindingState) filterBindingState {
	return filterBindingVisitor{}.VisitAnd(left, right)
}

func (filterBindingVisitor) VisitUnbound(iceberg.UnboundPredicate) filterBindingState {
	return filterBindingState{hasUnbound: true}
}

func (filterBindingVisitor) VisitBound(iceberg.BoundPredicate) filterBindingState {
	return filterBindingState{hasBound: true}
}

type boundFilterSchemaVisitor struct {
	schema *iceberg.Schema
}

func (boundFilterSchemaVisitor) VisitTrue() error  { return nil }
func (boundFilterSchemaVisitor) VisitFalse() error { return nil }
func (boundFilterSchemaVisitor) VisitNot(child error) error {
	return child
}

func firstFilterValidationError(left, right error) error {
	if left != nil {
		return left
	}

	return right
}

func (boundFilterSchemaVisitor) VisitAnd(left, right error) error {
	return firstFilterValidationError(left, right)
}

func (boundFilterSchemaVisitor) VisitOr(left, right error) error {
	return firstFilterValidationError(left, right)
}

func (boundFilterSchemaVisitor) VisitUnbound(iceberg.UnboundPredicate) error {
	return fmt.Errorf("%w: expected a fully bound scan task residual", iceberg.ErrInvalidArgument)
}

func (v boundFilterSchemaVisitor) VisitBound(pred iceberg.BoundPredicate) error {
	boundRef := pred.Ref()
	boundField := boundRef.Field()
	effectiveField, ok := v.schema.FindFieldByID(boundField.ID)
	if !ok {
		return fmt.Errorf("%w: bound residual references field ID %d, which is not present in the scan schema",
			iceberg.ErrInvalidArgument, boundField.ID)
	}
	if !effectiveField.Type.Equals(boundField.Type) {
		return fmt.Errorf("%w: bound residual field ID %d has type %s, but the scan schema has type %s",
			iceberg.ErrInvalidArgument, boundField.ID, boundField.Type, effectiveField.Type)
	}

	columnName, ok := v.schema.FindColumnName(boundField.ID)
	if !ok {
		return fmt.Errorf("%w: scan schema has no name for field ID %d",
			iceberg.ErrInvalidArgument, boundField.ID)
	}
	rebound, err := iceberg.Reference(columnName).Bind(v.schema, true)
	if err != nil {
		return fmt.Errorf("%w: cannot validate field ID %d against the scan schema: %v",
			iceberg.ErrInvalidArgument, boundField.ID, err)
	}
	if !slices.Equal(boundRef.PosPath(), rebound.Ref().PosPath()) {
		return fmt.Errorf("%w: bound residual field ID %d has accessor path %v, but the scan schema uses %v",
			iceberg.ErrInvalidArgument, boundField.ID, boundRef.PosPath(), rebound.Ref().PosPath())
	}

	return nil
}

func validateBoundFilter(schema *iceberg.Schema, filter iceberg.BooleanExpression) error {
	if schema == nil {
		return fmt.Errorf("%w: cannot validate a bound scan task residual against a nil schema",
			iceberg.ErrInvalidArgument)
	}

	validationErr, visitErr := iceberg.VisitExpr(filter, boundFilterSchemaVisitor{schema: schema})
	if visitErr != nil {
		return visitErr
	}

	return validationErr
}

// bindTaskFilter returns changed=true only when binding an unbound residual
// produces a new expression. When changed is false, the returned expression is
// the exact input value, after validating any already-bound predicates.
func bindTaskFilter(schema *iceberg.Schema, filter iceberg.BooleanExpression, caseSensitive bool) (iceberg.BooleanExpression, bool, error) {
	if filter == nil {
		return nil, false, nil
	}

	state, err := iceberg.VisitExpr(filter, filterBindingVisitor{})
	if err != nil {
		return nil, false, err
	}
	if state.hasBound && state.hasUnbound {
		return nil, false, fmt.Errorf("%w: scan task residual mixes bound and unbound predicates", iceberg.ErrInvalidArgument)
	}
	if !state.hasUnbound {
		if state.hasBound {
			if err := validateBoundFilter(schema, filter); err != nil {
				return nil, false, err
			}
		}

		return filter, false, nil
	}

	bound, err := iceberg.BindExpr(schema, filter, caseSensitive)
	if err != nil {
		return nil, false, err
	}

	return bound, true, nil
}

func (as *arrowScan) rowFilterForTask(task FileScanTask) (iceberg.BooleanExpression, error) {
	if task.Residual == nil {
		return as.boundRowFilter, nil
	}
	if as.taskResidualsBound {
		return task.Residual, nil
	}

	filterSchema := as.scanSchema
	if as.filterSchema != nil {
		filterSchema = as.filterSchema
	}

	bound, _, err := bindTaskFilter(filterSchema, task.Residual, as.caseSensitive)

	return bound, err
}

// fieldIndexByID returns the index of the field carrying fieldID in its Arrow
// metadata, or -1. Resolving by reserved id (not name) matches how
// SchemaWithRowLineageColumns and projection identify lineage columns.
func fieldIndexByID(schema *arrow.Schema, fieldID int) int {
	for i, f := range schema.Fields() {
		if v, ok := f.Metadata.GetValue(ArrowParquetFieldIDKey); ok {
			if id, err := strconv.Atoi(v); err == nil && id == fieldID {
				return i
			}
		}
	}

	return -1
}

// rowPositionSource holds the surviving row groups for a single file read.
// processRecords populates spans (via the row-group tester) before any record is
// read, then every position-keyed pipeline step takes its own cursor over the
// shared, immutable spans. One source, many cursors: each step advances
// independently but resolves the same original file positions.
//
// With no spans recorded (non-Parquet files, or a read with no pruning pass) the
// cursors fall back to a contiguous counter from zero.
type rowPositionSource struct {
	spans []tblutils.RowGroupSpan
}

func (s *rowPositionSource) cursor() *rowPositionCursor {
	return &rowPositionCursor{src: s}
}

// pruned reports whether any row group was skipped, i.e. whether emitted rows
// are non-contiguous. Read only after spans are populated.
func (s *rowPositionSource) pruned() bool {
	return len(s.spans) > 0
}

// rowPositionCursor maps each emitted row back to its original position in the
// data file. Surviving row groups are visited in file order, so positions jump
// across the gaps left by pruned groups, keeping _row_id = first_row_id +
// original position correct even while row-group pruning is active.
type rowPositionCursor struct {
	src      *rowPositionSource
	spanIdx  int
	consumed int64
}

// next returns the original file position of the next row. It is meant to be
// called once per physical row read, in order. spanIdx is bounded to the last
// span, so reading past the total row count degrades to positions beyond the
// final span rather than panicking here. Callers that index a buffer sized to
// the file's row count (e.g. filterByDeletionVector's keep-mask) must still
// guard against the returned position exceeding that count.
func (c *rowPositionCursor) next() int64 {
	spans := c.src.spans
	if len(spans) == 0 {
		pos := c.consumed
		c.consumed++

		return pos
	}

	for c.spanIdx < len(spans)-1 && c.consumed >= spans[c.spanIdx].NumRows {
		c.spanIdx++
		c.consumed = 0
	}

	span := spans[c.spanIdx]
	pos := span.FirstRowPos + c.consumed
	c.consumed++

	return pos
}

// advance consumes rows without materializing their positions. This is used
// when a row-id column is already complete and only cursor alignment matters.
// Keep the cursor at an exact span boundary until the next read, matching next.
func (c *rowPositionCursor) advance(rows int64) {
	if rows <= 0 {
		return
	}

	spans := c.src.spans
	if len(spans) == 0 {
		c.consumed += rows

		return
	}

	for c.spanIdx < len(spans)-1 {
		remaining := spans[c.spanIdx].NumRows - c.consumed
		if remaining <= 0 {
			c.spanIdx++
			c.consumed = 0

			continue
		}
		if rows <= remaining {
			c.consumed += rows

			return
		}

		rows -= remaining
		c.spanIdx++
		c.consumed = 0
	}
	c.consumed += rows
}

// synthesizeRowLineageColumns fills the requested row-lineage columns from task
// constants: per the v3 spec a null value inherits first_row_id + position /
// data_sequence_number, a non-null value is kept. A column absent from the batch
// is appended. The caller sets synthesizeRowID/synthesizeSeq only when the
// matching task constant is present.
//
// MUST run before any row-dropping step: _row_id is first_row_id + the row's
// ORIGINAL position, drawn from cursor (advanced once per row in the full batch),
// so it is only correct before rows are dropped. ToRequestedSchema then resolves
// the columns by reserved field id.
func synthesizeRowLineageColumns(
	ctx context.Context,
	cursor *rowPositionCursor,
	task FileScanTask,
	batch arrow.RecordBatch,
	synthesizeRowID, synthesizeSeq bool,
) (arrow.RecordBatch, error) {
	alloc := compute.GetAllocator(ctx)
	schema := batch.Schema()
	nrows := batch.NumRows()

	// Copy the schema and column lists only when a value must be synthesized.
	// Completely materialized lineage columns can reuse the entire input batch.
	var fields []arrow.Field
	var newCols []arrow.Array
	var built []arrow.Array
	defer func() {
		for _, a := range built {
			a.Release()
		}
	}()

	// The row-position cursor must consume every physical row, including
	// rows whose explicit value wins. A complete row-id column can advance
	// the cursor in bulk; other cases call perRow for each row.
	synth := func(name string, fieldID int, perRow func(k int64), value func(k int64) int64) error {
		idx := fieldIndexByID(schema, fieldID)
		var existing *array.Int64
		if idx >= 0 {
			var ok bool
			if existing, ok = batch.Column(idx).(*array.Int64); !ok {
				return fmt.Errorf("row-lineage column %s is %s, want int64", name, batch.Column(idx).DataType())
			}
		}
		if existing != nil && existing.NullN() == 0 {
			// The input already contains every value. In the row-id case,
			// advance past the batch without reconstructing each position.
			if perRow != nil {
				cursor.advance(nrows)
			}

			return nil
		}

		if newCols == nil {
			fields = append([]arrow.Field(nil), schema.Fields()...)
			newCols = append([]arrow.Array(nil), batch.Columns()...)
		}
		bldr := array.NewInt64Builder(alloc)
		defer bldr.Release()
		bldr.Reserve(int(nrows))
		for k := range nrows {
			if perRow != nil {
				perRow(k)
			}
			if existing != nil && !existing.IsNull(int(k)) {
				bldr.Append(existing.Value(int(k)))
			} else {
				bldr.Append(value(k))
			}
		}
		arr := bldr.NewArray()
		built = append(built, arr)

		if idx >= 0 {
			newCols[idx] = arr

			return nil
		}
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
			Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{strconv.Itoa(fieldID)}),
		})
		newCols = append(newCols, arr)

		return nil
	}

	if synthesizeRowID {
		first := *task.FirstRowID
		var pos int64
		if err := synth(iceberg.RowIDColumnName, iceberg.RowIDFieldID,
			func(int64) { pos = cursor.next() },
			func(int64) int64 { return first + pos },
		); err != nil {
			return nil, err
		}
	}
	if synthesizeSeq {
		seq := *task.DataSequenceNumber
		if err := synth(iceberg.LastUpdatedSequenceNumberColumnName, iceberg.LastUpdatedSequenceNumberFieldID, nil,
			func(int64) int64 {
				return seq
			}); err != nil {
			return nil, err
		}
	}

	if newCols == nil {
		batch.Retain()

		return batch, nil
	}

	meta := schema.Metadata()

	return array.NewRecordBatch(arrow.NewSchema(fields, &meta), newCols, nrows), nil
}

func (as *arrowScan) processRecords(
	ctx context.Context,
	task tblutils.Enumerated[FileScanTask],
	fileSchema *iceberg.Schema,
	rowFilter iceberg.BooleanExpression,
	rdr tblutils.FileReader,
	columns []int,
	pipeline []recProcessFn,
	posSource *rowPositionSource,
	sink *recordSink,
) error {
	return as.processRecordsWithPlans(ctx, task, fileSchema, rowFilter, rdr, columns, pipeline, posSource, sink, nil)
}

// adjustParquetTaskRange keeps a planned range within the physical file when
// manifest and footer sizes differ. It only clamps ranges that run past the
// physical end; caller-owned task boundaries must never be extended.
func adjustParquetTaskRange(task FileScanTask, physicalFileSize int64) (start, length int64) {
	start, length = task.Start, task.Length
	if task.File == nil || physicalFileSize <= 0 || start < 0 || start > physicalFileSize {
		return start, length
	}

	maxLength := physicalFileSize - start
	if length > maxLength {
		length = maxLength
	}

	return start, length
}

func (as *arrowScan) processRecordsWithPlans(
	ctx context.Context,
	task tblutils.Enumerated[FileScanTask],
	fileSchema *iceberg.Schema,
	rowFilter iceberg.BooleanExpression,
	rdr tblutils.FileReader,
	columns []int,
	pipeline []recProcessFn,
	posSource *rowPositionSource,
	sink *recordSink,
	plans *compiledFileFilterPlans,
) (err error) {
	var (
		testRowGroups any
		recRdr        array.RecordReader
	)
	pruningFilter := rowFilter
	if as.rowGroupFilter != nil {
		pruningFilter = as.rowGroupFilter
	}
	if pruningFilter == nil {
		pruningFilter = iceberg.AlwaysTrue{}
	}

	// Row-group stats/dictionary/bloom pruning skips whole groups, so emitted batches no
	// longer cover contiguous file positions. Steps that key on the original
	// position (row-lineage _row_id, positional/DV deletes, generated position
	// deletes) recover it from posSource, which the tester seeds with each
	// surviving group's file position before any record is read, so pruning
	// stays enabled.
	switch {
	case task.Value.File.FileFormat() == iceberg.ParquetFile:
		var tester *tblutils.ParquetRowGroupTester
		if plans != nil && plans.pruning != nil {
			tester = &tblutils.ParquetRowGroupTester{
				StatsFn:         plans.pruning.statsEvaluator(),
				StatsFieldIDs:   plans.pruning.statsFieldIDs,
				BloomPreds:      plans.pruning.bloomPreds,
				DictionaryPreds: plans.pruning.dictionaryPreds,
			}
		} else {
			logicalSchema := as.filterSchema
			if logicalSchema == nil {
				logicalSchema = as.projectedSchema
			}

			hasMissingDefault, err := pruningFilterHasMissingInitialDefault(
				pruningFilter, logicalSchema, fileSchema)
			if err != nil {
				return err
			}
			if hasMissingDefault {
				// TranslateColumnNames treats fields missing from the physical file
				// as null. That is unsafe for fields added with a non-null
				// initial-default because Iceberg materializes the default on read.
				// Keep the actual row filter, but conservatively skip early pruning.
				pruningFilter = iceberg.AlwaysTrue{}
			}

			pruningFilter, err = stripExtractPredicates(pruningFilter)
			if err != nil {
				return err
			}

			filePruningFilter, err := iceberg.TranslateColumnNames(pruningFilter, fileSchema)
			if err != nil {
				return err
			}

			filePruningFilter, err = iceberg.BindExpr(fileSchema, filePruningFilter, as.caseSensitive)
			if err != nil {
				return err
			}
			statsFieldIDs, err := iceberg.ExtractFieldIDs(filePruningFilter)
			if err != nil {
				return err
			}

			statsFn, err := newParquetRowGroupStatsEvaluator(fileSchema, filePruningFilter, false)
			if err != nil {
				return err
			}

			bloomPreds, err := newBloomFilterPredicates(filePruningFilter)
			if err != nil {
				return err
			}

			dictionaryPreds, err := newDictionaryPredicates(filePruningFilter)
			if err != nil {
				return err
			}

			tester = &tblutils.ParquetRowGroupTester{
				StatsFn:         statsFn,
				StatsFieldIDs:   statsFieldIDs,
				BloomPreds:      bloomPreds,
				DictionaryPreds: dictionaryPreds,
			}
		}
		// A complete task already reads every row group. Leave its byte range
		// unset so malformed or legacy row-group offsets cannot change the
		// historical full-file behavior. A zero-value task is also treated as
		// complete for callers that omit the range. Split and remote partial
		// tasks carry their range through to the reader.
		if task.Value.Start != 0 ||
			(task.Value.Length != 0 && task.Value.Length != task.Value.File.FileSizeBytes()) {
			tester.RangeSet = true
			tester.Start, tester.Length = adjustParquetTaskRange(task.Value, rdr.SourceFileSize())
			tester.PlanningSplitOffsets = iceinternal.BorrowedDataFileSplitOffsets(task.Value.File)
		}
		if posSource != nil {
			tester.Survivors = &posSource.spans
		}
		testRowGroups = tester
	}

	recRdr, err = rdr.GetRecords(ctx, columns, testRowGroups)
	if err != nil {
		return err
	}
	defer recRdr.Release()

	var (
		idx  int
		prev arrow.RecordBatch
	)

	for recRdr.Next() {
		if prev != nil {
			sink.send(enumeratedRecord{Record: tblutils.Enumerated[arrow.RecordBatch]{
				Value: prev, Index: idx, Last: false,
			}, Task: task})
			idx++
		}

		prev = recRdr.RecordBatch()
		prev.Retain()

		for _, f := range pipeline {
			prev, err = f(prev)
			if err != nil {
				return err
			}
		}
	}

	if prev != nil {
		sink.send(enumeratedRecord{Record: tblutils.Enumerated[arrow.RecordBatch]{
			Value: prev, Index: idx, Last: true,
		}, Task: task})
	} else {
		// The reader produced no batches (e.g. every row group was pruned by
		// stats). The sequenced channel in createIterator still needs this
		// task's Last record to release the following tasks' records, so emit
		// an empty batch just like the dropFile path does; without it every
		// record queued behind this task is silently discarded on close.
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		sink.send(enumeratedRecord{Record: tblutils.Enumerated[arrow.RecordBatch]{
			Value: array.NewRecordBatch(emptySchema, nil, 0), Index: idx, Last: true,
		}, Task: task})
	}

	if recRdr.Err() != nil && recRdr.Err() != io.EOF {
		err = recRdr.Err()
	}

	return err
}

func pruningFilterHasMissingInitialDefault(
	filter iceberg.BooleanExpression,
	logicalSchema, fileSchema *iceberg.Schema,
) (bool, error) {
	if logicalSchema == nil || fileSchema == nil {
		return false, nil
	}

	fieldIDs, err := iceberg.ExtractFieldIDs(filter)
	if err != nil {
		return false, err
	}

	for _, fieldID := range fieldIDs {
		if _, found := fileSchema.FindFieldByID(fieldID); found {
			continue
		}

		field, found := logicalSchema.FindFieldByID(fieldID)
		if found && field.InitialDefault != nil {
			return true, nil
		}
	}

	return false, nil
}

// canReuseFlatProjection detects the common case of flat, already-typed
// batches. Full schema projection still handles nested types, schema evolution,
// promotions, defaults, and changed column ordering. A matching Arrow type alone
// is not enough: the file field ID and Iceberg type must also agree.
func canReuseFlatProjection(requested, fileSchema *iceberg.Schema, input, output *arrow.Schema) bool {
	if requested == nil || fileSchema == nil || input == nil || output == nil {
		return false
	}

	fields := requested.FieldsRef(iceinternal.SchemaRef{})
	if len(fields) != input.NumFields() || len(fields) != output.NumFields() {
		return false
	}

	for i, field := range fields {
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return false
		}
		fileField, ok := fileSchema.FindFieldByIDRef(field.ID, iceinternal.SchemaRef{})
		if !ok || fileField.Name != field.Name || !fileField.Type.Equals(field.Type) {
			return false
		}

		source, target := input.Field(i), output.Field(i)
		if source.Name != field.Name || target.Name != field.Name ||
			!arrow.TypeEqual(source.Type, target.Type) || len(input.FieldIndices(field.Name)) != 1 {
			return false
		}
		if id, ok := source.Metadata.GetValue(ArrowParquetFieldIDKey); ok {
			parsed, err := strconv.Atoi(id)
			if err != nil || parsed != field.ID {
				return false
			}
		}
	}

	return true
}

func (as *arrowScan) recordsFromTask(ctx context.Context, task tblutils.Enumerated[FileScanTask], sink *recordSink, positionalDeleteBitmap *dv.RoaringPositionBitmap, dvBitmap *dv.RoaringPositionBitmap, eqDeleteSets []*equalityDeleteSet, invariants *arrowScanInvariants) (err error) {
	defer func() {
		if err != nil {
			sink.fail(task, err)
		}
	}()

	var (
		rowFilter   iceberg.BooleanExpression
		rdr         tblutils.FileReader
		iceSchema   *iceberg.Schema
		colIndices  []int
		filterFunc  recProcessFn
		dropFile    bool
		filterPlans *compiledFileFilterPlans
	)

	rowFilter, err = as.rowFilterForTask(task.Value)
	if err != nil {
		return err
	}

	iceSchema, colIndices, rdr, err = as.prepareToRead(ctx, task.Value.File, invariants)
	if err != nil {
		return err
	}
	defer iceinternal.CheckedClose(rdr, &err)
	if task.Value.Residual == nil {
		filterPlans, err = as.cachedFileFilterPlans(
			iceSchema, task.Value.File.FileFormat() == iceberg.ParquetFile)
		if err != nil {
			return err
		}
	}

	pipeline := make([]recProcessFn, 0, 4)

	// Synthesize lineage before any row-dropping step so deletes/filters can't
	// renumber survivors' _row_id; readSchema carries the field ids for
	// ToRequestedSchema. Gated so non-lineage scans keep the allocation-free path.
	readSchema := iceSchema
	rowLineageEnabled, lerr := strconv.ParseBool(as.options.Get(ScanOptionRowLineageEnabled, "true"))
	if lerr != nil {
		rowLineageEnabled = true
	}
	_, wantRowID := as.projectedSchema.FindFieldByID(iceberg.RowIDFieldID)
	_, wantSeqNum := as.projectedSchema.FindFieldByID(iceberg.LastUpdatedSequenceNumberFieldID)
	synthesizeRowID := rowLineageEnabled && wantRowID && task.Value.FirstRowID != nil
	synthesizeSeq := rowLineageEnabled && wantSeqNum && task.Value.DataSequenceNumber != nil

	// Every position-keyed step (_row_id synthesis, positional/DV deletes) needs
	// each row's original file position; under row-group pruning the emitted rows
	// are non-contiguous. processRecords seeds one shared source with the
	// surviving groups and each step takes its own cursor over it.
	hasDV := dvBitmap != nil && !dvBitmap.IsEmpty()
	// Per the v3 spec a DV supersedes positional delete files for a data file, so
	// when both are attached (only possible on a hand-built task; PlanFiles never
	// produces it) the DV wins and positional deletes are ignored. This mirrors
	// the planner (scanner.go) and Java's DeleteFileIndex, and keeps at most one
	// row-dropping position step in the pipeline so cursors never run over a
	// sequence an earlier step already shortened.
	applyPosDeletes := positionalDeleteBitmap != nil && !positionalDeleteBitmap.IsEmpty() && !hasDV
	var posSource *rowPositionSource
	if synthesizeRowID || applyPosDeletes || hasDV {
		posSource = &rowPositionSource{}
	}

	if synthesizeRowID || synthesizeSeq {
		// Mirror the columns synthesize will append so ToRequestedSchema never
		// resolves a lineage field missing from the batch.
		readSchema = iceberg.SchemaWithRowLineageColumns(iceSchema, synthesizeRowID, synthesizeSeq)
		taskVal := task.Value
		// Only _row_id depends on position, so a cursor is taken only then.
		var cursor *rowPositionCursor
		if synthesizeRowID {
			cursor = posSource.cursor()
		}
		pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
			defer r.Release()

			return synthesizeRowLineageColumns(ctx, cursor, taskVal, r, synthesizeRowID, synthesizeSeq)
		})
	}

	if applyPosDeletes {
		pipeline = append(pipeline,
			processPositionalDeleteBitmap(ctx, positionalDeleteBitmap, posSource.cursor()))
	}

	if hasDV {
		pipeline = append(pipeline, filterByDeletionVector(ctx, dvBitmap, task.Value.File.Count(), posSource.cursor()))
	}

	if len(eqDeleteSets) > 0 {
		eqFn, eqErr := processEqualityDeletesColumnarForFile(ctx, eqDeleteSets, iceSchema, task.Value.File.FilePath())
		if eqErr != nil {
			return eqErr
		}

		pipeline = append(pipeline, eqFn)
	}

	if filterPlans != nil {
		filterFunc, dropFile, err = as.getRecordFilterFromPlan(ctx, filterPlans.record)
	} else {
		filterFunc, dropFile, err = as.getRecordFilter(ctx, iceSchema, rowFilter)
	}
	if err != nil {
		return err
	}

	if dropFile {
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(as.projectedSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		sink.send(enumeratedRecord{Task: task, Record: tblutils.Enumerated[arrow.RecordBatch]{
			Value: array.NewRecordBatch(emptySchema, nil, 0), Index: 0, Last: true,
		}})

		return err
	}

	if filterFunc != nil {
		pipeline = append(pipeline, filterFunc)
	}

	var lastInputSchema *arrow.Schema
	var reuseFlatProjection bool
	pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		if r.Schema() != lastInputSchema {
			lastInputSchema = r.Schema()
			reuseFlatProjection = canReuseFlatProjection(
				as.projectedSchema, readSchema, lastInputSchema, invariants.outputArrowSchema)
		}
		if reuseFlatProjection {
			// Rewrap existing columns with the projected field metadata. Do not
			// rebuild the arrays or walk the Iceberg schema for every batch.
			return array.NewRecordBatch(invariants.outputArrowSchema, r.Columns(), r.NumRows()), nil
		}

		return ToRequestedSchema(ctx, as.projectedSchema, readSchema, r, SchemaOptions{
			UseLargeTypes:   as.useLargeTypes,
			TableProperties: invariants.tableProperties,
		})
	})

	err = as.processRecordsWithPlans(ctx, task, iceSchema, rowFilter, rdr, colIndices, pipeline, posSource, sink, filterPlans)

	return err
}

func (as *arrowScan) producePosDeletesFromTask(ctx context.Context, task tblutils.Enumerated[FileScanTask], positionalDeletes positionDeletes, sink *recordSink, invariants *arrowScanInvariants) (err error) {
	defer func() {
		if err != nil {
			sink.fail(task, err)
		}
	}()

	var (
		rdr         tblutils.FileReader
		iceSchema   *iceberg.Schema
		colIndices  []int
		filterFunc  recProcessFn
		dropFile    bool
		filterPlans *compiledFileFilterPlans
	)

	iceSchema, colIndices, rdr, err = as.prepareToRead(ctx, task.Value.File, invariants)
	if err != nil {
		return err
	}
	defer iceinternal.CheckedClose(rdr, &err)
	filterPlans, err = as.cachedFileFilterPlans(
		iceSchema, task.Value.File.FileFormat() == iceberg.ParquetFile)
	if err != nil {
		return err
	}

	fields := append(iceSchema.Fields(), iceberg.PositionalDeleteSchema.Fields()...)
	enrichedIcebergSchema := iceberg.NewSchema(iceSchema.ID+1, fields...)

	// Each row's original file position is stamped into the generated delete
	// records and matched against any pre-existing positional deletes. Under the
	// delete filter, pruned row groups hold no matching rows, so skipping them
	// drops no generated deletes; cursors over the shared source keep the
	// emitted positions correct across the gaps.
	posSource := &rowPositionSource{}
	pipeline := make([]recProcessFn, 0, 2)
	pipeline = append(pipeline, enrichRecordsWithPosDeleteFields(ctx, task.Value.File, posSource.cursor()))
	if len(positionalDeletes) > 0 {
		deletes, err := collectPosDeletePositions(positionalDeletes)
		if err != nil {
			return err
		}

		pipeline = append(pipeline, processPositionalDeletes(ctx, deletes, posSource.cursor()))
	}

	filterFunc, dropFile, err = as.getRecordFilterFromPlan(ctx, filterPlans.record)
	if err != nil {
		return err
	}

	// Nothing to delete in a dropped file
	if dropFile {
		var emptySchema *arrow.Schema
		emptySchema, err = SchemaToArrowSchema(iceberg.PositionalDeleteSchema, nil, false, as.useLargeTypes)
		if err != nil {
			return err
		}
		sink.send(enumeratedRecord{Task: task, Record: tblutils.Enumerated[arrow.RecordBatch]{
			Value: array.NewRecordBatch(emptySchema, nil, 0), Index: 0, Last: true,
		}})

		return err
	}

	if filterFunc != nil {
		pipeline = append(pipeline, filterFunc)
	}
	pipeline = append(pipeline, func(r arrow.RecordBatch) (arrow.RecordBatch, error) {
		defer r.Release()

		return ToRequestedSchema(ctx, iceberg.PositionalDeleteSchema, enrichedIcebergSchema, r, SchemaOptions{IncludeFieldIDs: true, UseLargeTypes: as.useLargeTypes})
	})

	err = as.processRecordsWithPlans(ctx, task, iceSchema, as.boundRowFilter, rdr, colIndices, pipeline, posSource, sink, filterPlans)

	return err
}

func createIterator(ctx context.Context, numWorkers uint, records <-chan enumeratedRecord, deletesPerFile perFilePosDeletes, cancel context.CancelCauseFunc, rowLimit int64) iter.Seq2[arrow.RecordBatch, error] {
	return createIteratorWithCleanup(ctx, numWorkers, records, deletesPerFile, cancel, rowLimit, nil)
}

func createIteratorWithCleanup(ctx context.Context, numWorkers uint, records <-chan enumeratedRecord, deletesPerFile perFilePosDeletes, cancel context.CancelCauseFunc, rowLimit int64, cleanup func()) iter.Seq2[arrow.RecordBatch, error] {
	isBeforeAny := func(batch enumeratedRecord) bool {
		return batch.Task.Index < 0
	}

	sequenced := tblutils.MakeSequencedChanWithDiscard(uint(numWorkers), records,
		func(left, right *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*left):
				return true
			case isBeforeAny(*right):
				return false
			case left.Err != nil || right.Err != nil:
				return true
			case left.Task.Index == right.Task.Index:
				return left.Record.Index < right.Record.Index
			default:
				return left.Task.Index < right.Task.Index
			}
		}, func(prev, next *enumeratedRecord) bool {
			switch {
			case isBeforeAny(*prev):
				return next.Task.Index == 0 && next.Record.Index == 0
			case next.Err != nil:
				return true
			case prev.Task.Index == next.Task.Index:
				return next.Record.Index == prev.Record.Index+1
			default:
				return next.Task.Index == prev.Task.Index+1 &&
					prev.Record.Last && next.Record.Index == 0
			}
		}, enumeratedRecord{Task: tblutils.Enumerated[FileScanTask]{Index: -1}}, func(rec enumeratedRecord) {
			rec.credits.release()
			if rec.Record.Value != nil {
				rec.Record.Value.Release()
			}
		})

	totalRowCount := int64(0)

	return func(yield func(arrow.RecordBatch, error) bool) {
		defer func() {
			for rec := range sequenced {
				rec.credits.release()
				if rec.Record.Value != nil {
					rec.Record.Value.Release()
				}
			}

			releasePerFilePosDeletes(deletesPerFile)
			if cleanup != nil {
				cleanup()
			}
		}()

		defer cancel(nil)

		for {
			select {
			case <-ctx.Done():
				if err := context.Cause(ctx); err != nil {
					yield(nil, err)
				}

				return
			case enum, ok := <-sequenced:
				if !ok {
					if err := context.Cause(ctx); err != nil {
						yield(nil, err)
					}

					return
				}

				enum.credits.release()
				if enum.Err != nil {
					yield(nil, enum.Err)

					return
				}

				rec := enum.Record.Value
				if rowLimit > 0 {
					if totalRowCount >= rowLimit {
						rec.Release()

						return
					} else if totalRowCount+rec.NumRows() > rowLimit {
						defer rec.Release()
						rec = rec.NewSlice(0, rowLimit-totalRowCount)
					}
				}

				if rec.NumRows() == 0 {
					// skip empty records
					rec.Release()

					continue
				}

				if !yield(rec, nil) {
					return
				}
				totalRowCount += rec.NumRows()
				if rowLimit > 0 && totalRowCount >= rowLimit {
					return
				}
			}
		}
	}
}

// newScanTaskChannel buffers enough work to keep scan workers busy without
// copying the entire task list into the channel for large scans.
func newScanTaskChannel(taskCount, numWorkers int) chan tblutils.Enumerated[FileScanTask] {
	return make(chan tblutils.Enumerated[FileScanTask], min(taskCount, 2*numWorkers))
}

func (as *arrowScan) recordBatchesFromTasksAndDeletes(ctx context.Context, tasks []FileScanTask, positionDeleteLoader *lazyPositionDeleteLoader, dvLoader *lazyDeletionVectorLoader, equalityDeleteLoader *lazyEqualityDeleteLoader, invariants *arrowScanInvariants) iter.Seq2[arrow.RecordBatch, error] {
	return func(yield func(arrow.RecordBatch, error) bool) {
		extSet := substrait.NewExtensionSet()
		scanCtx, cancel := context.WithCancelCause(exprs.WithExtensionIDSet(ctx, extSet))
		numWorkers := min(as.concurrency, len(tasks))
		taskChan := newScanTaskChannel(len(tasks), numWorkers)
		records := make(chan enumeratedRecord, numWorkers)

		var wg sync.WaitGroup
		wg.Add(numWorkers)
		for range numWorkers {
			go func() {
				defer wg.Done()
				sink := newRecordSink(records)
				for {
					if err := sink.reserve(scanCtx); err != nil {
						return
					}
					select {
					case <-scanCtx.Done():
						return
					case task, ok := <-taskChan:
						if !ok {
							return
						}
						if scanCtx.Err() != nil {
							return
						}

						filePath := task.Value.File.FilePath()
						var positionalDeleteBitmap *dv.RoaringPositionBitmap
						if positionDeleteLoader != nil {
							var err error
							positionalDeleteBitmap, err = positionDeleteLoader.load(scanCtx, task.Index, filePath)
							if err != nil {
								sink.fail(task, err)
								cancel(err)

								return
							}
						}

						dvBitmap, err := dvLoader.load(scanCtx, filePath)
						if err != nil {
							sink.fail(task, err)
							cancel(err)

							return
						}
						eqDeleteSets, err := equalityDeleteLoader.load(scanCtx, task.Value)
						if err != nil {
							sink.fail(task, err)
							cancel(err)

							return
						}

						if err := as.recordsFromTask(scanCtx, task, sink,
							positionalDeleteBitmap,
							dvBitmap,
							eqDeleteSets,
							invariants); err != nil {
							cancel(err)

							return
						}
					}
				}
			}()
		}

		go func() {
			defer func() {
				close(taskChan)
				wg.Wait()
				close(records)
			}()

			for i, t := range tasks {
				select {
				case <-scanCtx.Done():
					return
				case taskChan <- tblutils.Enumerated[FileScanTask]{
					Value: t, Index: i, Last: i == len(tasks)-1,
				}:
				}
			}
		}()

		var cleanup func()
		if positionDeleteLoader != nil {
			cleanup = positionDeleteLoader.release
		}
		createIteratorWithCleanup(scanCtx, uint(numWorkers), records, nil,
			cancel, as.rowLimit, cleanup)(yield)
	}
}

// GetRecords prepares the projected Arrow schema and a single-use record
// iterator. Positional- and equality-delete files are opened and read during
// iteration. Errors from those files are delivered through the iterator only if
// iteration reaches the task that encounters the error; a row limit or early
// termination may finish the scan before the error is observed. Deletion-vector
// errors are returned before the iterator is created.
func (as *arrowScan) GetRecords(ctx context.Context, tasks []FileScanTask) (*arrow.Schema, iter.Seq2[arrow.RecordBatch, error], error) {
	var err error
	as.useLargeTypes, err = strconv.ParseBool(as.options.Get(ScanOptionArrowUseLargeTypes, "false"))
	if err != nil {
		as.useLargeTypes = false
	}

	tableProperties := as.metadata.Properties()
	batchSize := as.options.Get(ParquetBatchSizeKey, "")
	if as.arrowBatchSize > 0 {
		batchSize = strconv.Itoa(as.arrowBatchSize)
	}
	if batchSize != "" {
		tableProperties = maps.Clone(tableProperties)
		if tableProperties == nil {
			tableProperties = iceberg.Properties{}
		}
		tableProperties[ParquetBatchSizeKey] = batchSize
	}
	ctx = tblutils.WithTableProperties(ctx, tableProperties)

	resultSchema, err := SchemaToArrowSchemaWithOptions(as.projectedSchema, ArrowSchemaOptions{
		UseLargeTypes:   as.useLargeTypes,
		TableProperties: tableProperties,
	})
	if err != nil {
		return nil, nil, err
	}

	if as.rowLimit == 0 {
		return resultSchema, func(yield func(arrow.RecordBatch, error) bool) {}, nil
	}

	if len(tasks) > 1 {
		as.cacheFileReadPlan = true
		ctx = tblutils.WithParquetMetadataCache(ctx)
	} else {
		as.cacheFileReadPlan = false
	}

	invariants, err := as.scanInvariants(tableProperties)
	if err != nil {
		return nil, nil, err
	}
	invariants.outputArrowSchema = resultSchema
	if err := as.addTaskProjectedFieldIDs(invariants, tasks); err != nil {
		return nil, nil, err
	}

	// Index DV ownership up front, but defer Puffin reads until a task using a
	// referenced data file enters the iterator. The loader keeps each shared
	// Puffin group cached after its first read.
	dvLoader, err := newLazyDeletionVectorLoader(as.fs, tasks)
	if err != nil {
		return nil, nil, err
	}

	equalityDeleteLoader, err := newLazyEqualityDeleteLoader(
		as.fs, invariants.tableSchema, nil, invariants.nameMapping, tasks)
	if err != nil {
		return nil, nil, err
	}
	if equalityDeleteLoader.needsSchemaHistory() {
		equalityDeleteLoader.tableSchemas = as.metadata.Schemas()
	}
	equalityDeleteLoader.addFieldIDs(invariants.projectedIDs)

	positionDeleteLoader := newLazyPositionDeleteLoader(as.fs, tasks)

	return resultSchema, as.recordBatchesFromTasksAndDeletes(ctx, tasks,
		positionDeleteLoader, dvLoader, equalityDeleteLoader, invariants), nil
}

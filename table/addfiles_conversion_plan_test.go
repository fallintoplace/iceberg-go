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
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type addFilesConversionFixture struct {
	schema *iceberg.Schema
	spec iceberg.PartitionSpec
	path string
	fs iceio.LocalFS
	props iceberg.Properties
}

func makeAddFilesConversionFixture(tb testing.TB, width int, withIDs bool) addFilesConversionFixture {
	tb.Helper()
	fields := make([]iceberg.NestedField, width)
	arrowFields := make([]arrow.Field, width)
	for i := range fields {
		name := fmt.Sprintf("column_%03d", i)
		fields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: iceberg.PrimitiveTypes.Int64}
		arrowFields[i] = arrow.Field{Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	}
	schema := iceberg.NewSchema(1, fields...)
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	if withIDs {
		var err error
		arrowSchema, err = SchemaToArrowSchema(schema, nil, true, false)
		require.NoError(tb, err)
	}

	cols := make([]arrow.Array, width)
	for i := range cols {
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.Append(int64(i + 1))
		cols[i] = builder.NewArray()
		builder.Release()
	}
	rec := array.NewRecordBatch(arrowSchema, cols, 1)
	for _, col := range cols {
		col.Release()
	}
	defer rec.Release()
	table := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{rec})
	defer table.Release()

	path := filepath.Join(tb.TempDir(), "source.parquet")
	fs := iceio.LocalFS{}
	output, err := fs.Create(path)
	require.NoError(tb, err)
	require.NoError(tb, pqarrow.WriteTable(table, output, table.NumRows(), nil, pqarrow.DefaultWriterProps()))
	require.NoError(tb, output.Close())

	return addFilesConversionFixture{
		schema: schema,
		spec: *iceberg.UnpartitionedSpec,
		path: path,
		fs: fs,
		props: iceberg.Properties{
			MetricsModeColumnConfPrefix + ".column_000": "counts",
		},
	}
}

func TestAddFilesConversionPlanParity(t *testing.T) {
	ctx := context.Background()
	for _, withIDs := range []bool{false, true} {
		t.Run(fmt.Sprintf("field-ids=%t", withIDs), func(t *testing.T) {
			fixture := makeAddFilesConversionFixture(t, 16, withIDs)
			plan, err := newAddFilesConversionPlan(fixture.schema, fixture.props)
			require.NoError(t, err)

			baseline := fileToDataFile(ctx, fixture.fs, fixture.path, fixture.schema, fixture.spec, 0, fixture.props)
			reused := fileToDataFileWithPlan(ctx, fixture.fs, fixture.path, fixture.schema, fixture.spec, 0, fixture.props, plan)
			require.Equal(t, baseline.FilePath(), reused.FilePath())
			require.Equal(t, baseline.Count(), reused.Count())
			require.Equal(t, baseline.FileSizeBytes(), reused.FileSizeBytes())
			require.Equal(t, baseline.ColumnSizes(), reused.ColumnSizes())
			require.Equal(t, baseline.ValueCounts(), reused.ValueCounts())
			require.Equal(t, baseline.NullValueCounts(), reused.NullValueCounts())
			require.Equal(t, baseline.LowerBoundValues(), reused.LowerBoundValues())
			require.Equal(t, baseline.UpperBoundValues(), reused.UpperBoundValues())
			require.Equal(t, baseline.SplitOffsets(), reused.SplitOffsets())

			meta, err := NewMetadata(fixture.schema, &fixture.spec, UnsortedSortOrder, filepath.Dir(fixture.path), fixture.props)
			require.NoError(t, err)
			builder, err := MetadataBuilderFromBase(meta, "")
			require.NoError(t, err)
			files, err := filesToDataFiles(ctx, fixture.fs, builder,
				[]string{fixture.path, fixture.path, fixture.path, fixture.path}, 4)
			require.NoError(t, err)
			require.Len(t, files, 4)
			for _, file := range files {
				require.Equal(t, baseline.ColumnSizes(), file.ColumnSizes())
				require.Equal(t, baseline.LowerBoundValues(), file.LowerBoundValues())
			}
		})
	}
}

func TestAddFilesConversionPlanConcurrentReads(t *testing.T) {
	fixture := makeAddFilesConversionFixture(t, 32, false)
	plan, err := newAddFilesConversionPlan(fixture.schema, fixture.props)
	require.NoError(t, err)
	ctx := context.Background()
	baseline := fileToDataFile(ctx, fixture.fs, fixture.path, fixture.schema, fixture.spec, 0, fixture.props)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 4 {
				actual := fileToDataFileWithPlan(ctx, fixture.fs, fixture.path, fixture.schema, fixture.spec, 0, fixture.props, plan)
				assert.Equal(t, baseline.ColumnSizes(), actual.ColumnSizes())
				assert.Equal(t, baseline.ValueCounts(), actual.ValueCounts())
			}
		})
	}
	wg.Wait()
}

func TestAddFilesConversionPlanRejectsInvalidMode(t *testing.T) {
	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	_, err := newAddFilesConversionPlan(schema, iceberg.Properties{DefaultWriteMetricsModeKey: "invalid-mode"})
	require.Error(t, err)
}

var addFilesConversionBenchmarkSink iceberg.DataFile

func BenchmarkAddFilesConversionPlans(b *testing.B) {
	ctx := context.Background()
	for _, width := range []int{32, 256} {
		fixture := makeAddFilesConversionFixture(b, width, false)
		for _, count := range []int{8, 64} {
			b.Run(fmt.Sprintf("columns=%d/files=%d", width, count), func(b *testing.B) {
				b.Run("per-file", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						for range count {
							addFilesConversionBenchmarkSink = fileToDataFile(
								ctx, fixture.fs, fixture.path, fixture.schema, fixture.spec, 0, fixture.props)
						}
					}
					b.ReportMetric(float64(count), "files/op")
				})
				b.Run("shared-plan", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						plan, err := newAddFilesConversionPlan(fixture.schema, fixture.props)
						if err != nil {
							b.Fatal(err)
						}
						for range count {
							addFilesConversionBenchmarkSink = fileToDataFileWithPlan(
								ctx, fixture.fs, fixture.path, fixture.schema, fixture.spec, 0, fixture.props, plan)
						}
					}
					b.ReportMetric(float64(count), "files/op")
				})
			})
		}
	}
}

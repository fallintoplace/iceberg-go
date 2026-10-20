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
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func flatReadTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
}

func flatReadTestBatch(mem memory.Allocator, id int32, idType arrow.DataType, orderReversed bool) arrow.RecordBatch {
	fields := []arrow.Field{
		{Name: "id", Type: idType, Nullable: true, Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey, "source"}, []string{strconv.FormatInt(int64(id), 10), "parquet"})},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{"2"})},
	}
	if orderReversed {
		fields[0], fields[1] = fields[1], fields[0]
	}
	bldr := array.NewRecordBuilder(mem, arrow.NewSchema(fields, nil))
	defer bldr.Release()
	for i, field := range fields {
		switch field.Name {
		case "id":
			switch builder := bldr.Field(i).(type) {
			case *array.Int64Builder:
				builder.AppendValues([]int64{10, 20, 30}, []bool{true, false, true})
			case *array.Int32Builder:
				builder.AppendValues([]int32{10, 20, 30}, []bool{true, false, true})
			}
		case "name":
			bldr.Field(i).(*array.StringBuilder).AppendValues([]string{"a", "b", "c"}, nil)
		}
	}
	return bldr.NewRecordBatch()
}

func TestFlatReadProjectionMatchesFullVisitor(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := flatReadTestSchema()
	record := flatReadTestBatch(mem, 1, arrow.PrimitiveTypes.Int64, false)
	defer record.Release()

	plan, err := newFlatReadProjection(schema, schema, false, nil)
	require.NoError(t, err)
	require.NotNil(t, plan)

	projected, ok := plan.project(record)
	require.True(t, ok)
	defer projected.Release()

	expected, err := ToRequestedSchema(t.Context(), schema, schema, record, SchemaOptions{})
	require.NoError(t, err)
	defer expected.Release()

	assert.True(t, array.RecordEqual(expected, projected))
	assert.True(t, arrowSchemaEqual(expected.Schema(), projected.Schema()))
	assert.False(t, projected.Schema().Field(0).HasMetadata(), "Parquet field IDs and extra metadata must be stripped")
	assert.Equal(t, int64(3), projected.NumRows())
	assert.Equal(t, 1, projected.Column(0).NullN())
}

func TestFlatReadProjectionReusesExactBatch(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	schema := flatReadTestSchema()
	target, err := SchemaToArrowSchema(schema, nil, false, false)
	require.NoError(t, err)
	bldr := array.NewRecordBuilder(mem, target)
	bldr.Field(0).(*array.Int64Builder).Append(42)
	bldr.Field(1).(*array.StringBuilder).Append("ok")
	record := bldr.NewRecordBatch()
	bldr.Release()
	defer record.Release()

	plan, err := newFlatReadProjection(schema, schema, false, nil)
	require.NoError(t, err)
	projected, ok := plan.project(record)
	require.True(t, ok)
	assert.Same(t, record, projected)
	projected.Release()
}

func TestFlatReadProjectionFallsBackOnMismatch(t *testing.T) {
	schema := flatReadTestSchema()
	plan, err := newFlatReadProjection(schema, schema, false, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		name          string
		id            int32
		idType        arrow.DataType
		orderReversed bool
	}{
		{name: "wrong field ID", id: 3, idType: arrow.PrimitiveTypes.Int64},
		{name: "wrong physical type", id: 1, idType: arrow.PrimitiveTypes.Int32},
		{name: "reordered fields", id: 1, idType: arrow.PrimitiveTypes.Int64, orderReversed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			record := flatReadTestBatch(mem, tc.id, tc.idType, tc.orderReversed)
			defer record.Release()

			_, ok := plan.project(record)
			assert.False(t, ok)
		})
	}

	t.Run("evolved field ID", func(t *testing.T) {
		physical := iceberg.NewSchema(0,
			iceberg.NestedField{ID: 3, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		)
		p, err := newFlatReadProjection(schema, physical, false, nil)
		require.NoError(t, err)
		require.Nil(t, p)
	})
	t.Run("missing projected field", func(t *testing.T) {
		physical := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
		p, err := newFlatReadProjection(schema, physical, false, nil)
		require.NoError(t, err)
		require.Nil(t, p)
	})
	t.Run("empty schema", func(t *testing.T) {
		empty := iceberg.NewSchema(0)
		p, err := newFlatReadProjection(empty, empty, false, nil)
		require.NoError(t, err)
		require.Nil(t, p)
	})
	t.Run("nested field", func(t *testing.T) {
		nested := iceberg.NewSchema(0, iceberg.NestedField{
			ID: 1, Name: "person", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
				{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
			}},
		})
		p, err := newFlatReadProjection(nested, nested, false, nil)
		require.NoError(t, err)
		require.Nil(t, p)
	})
}

var flatReadProjectionBenchmarkSink int64

func BenchmarkFlatReadProjection(b *testing.B) {
	for _, fieldCount := range []int{8, 64, 256} {
		b.Run(fmt.Sprintf("fields=%d", fieldCount), func(b *testing.B) {
			mem := memory.DefaultAllocator
			iceFields := make([]iceberg.NestedField, fieldCount)
			arrowFields := make([]arrow.Field, fieldCount)
			for i := range iceFields {
				name := fmt.Sprintf("col_%d", i)
				iceFields[i] = iceberg.NestedField{ID: i + 1, Name: name, Type: iceberg.PrimitiveTypes.Int64}
				arrowFields[i] = arrow.Field{
					Name: name, Type: arrow.PrimitiveTypes.Int64, Nullable: true,
					Metadata: arrow.NewMetadata([]string{ArrowParquetFieldIDKey}, []string{strconv.Itoa(i + 1)}),
				}
			}
			schema := iceberg.NewSchema(0, iceFields...)
			bldr := array.NewRecordBuilder(mem, arrow.NewSchema(arrowFields, nil))
			for i := range iceFields {
				bldr.Field(i).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
			}
			record := bldr.NewRecordBatch()
			bldr.Release()
			defer record.Release()

			plan, err := newFlatReadProjection(schema, schema, false, nil)
			if err != nil || plan == nil {
				b.Fatalf("prepare flat projection: %v", err)
			}
			b.Run("visitor", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, err := ToRequestedSchema(context.Background(), schema, schema, record, SchemaOptions{})
					if err != nil {
						b.Fatal(err)
					}
					flatReadProjectionBenchmarkSink = out.NumRows()
					out.Release()
				}
			})
			b.Run("flat", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					out, ok := plan.project(record)
					if !ok {
						b.Fatal("unexpected slow path")
					}
					flatReadProjectionBenchmarkSink = out.NumRows()
					out.Release()
				}
			})
		})
	}
}

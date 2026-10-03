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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadEqualityDeleteFileRefreshesBatchEncoders(t *testing.T) {
	t.Parallel()
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
		iceberg.NestedField{ID: 3, Name: "nested", Type: &iceberg.StructType{FieldList: []iceberg.NestedField{
			{ID: 4, Name: "value", Type: iceberg.PrimitiveTypes.Int64},
		}}},
	)
	arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
	require.NoError(t, err)
	for _, tt := range []struct{ name, content string }{
		{"empty", `[]`},
		{"values and nulls", `[
   {"id": 1, "name": "first", "nested": {"value": 10}},
   {"id": null, "name": "second", "nested": null},
   {"id": 3, "name": null, "nested": {"value": null}},
   {"id": 4, "name": "last", "nested": {"value": 40}},
   {"id": 1, "name": "first", "nested": {"value": 10}}
  ]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record := mustLoadRecordBatchFromJSON(arrowSchema, tt.content)
			defer record.Release()
			tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
			defer tbl.Release()
			path := filepath.Join(t.TempDir(), "equality-delete.parquet")
			file, err := (iceio.LocalFS{}).Create(path)
			require.NoError(t, err)
			require.NoError(t, pqarrow.WriteTable(tbl, file, max(1, record.NumRows()),
				parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
			info, err := os.Stat(path)
			require.NoError(t, err)
			builder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
				path, iceberg.ParquetFile, nil, nil, nil, record.NumRows(), info.Size())
			require.NoError(t, err)
			fieldIDs := []int{1, 2, 4}
			builder.EqualityFieldIDs(fieldIDs)
			dataFile := builder.Build()
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			ctx := compute.WithAllocator(t.Context(), mem)
			wantKeys, wantNames, err := readEqualityDeleteFileMaterialized(ctx, iceio.LocalFS{}, schema, nil, dataFile, fieldIDs)
			require.NoError(t, err)
			assert.Len(t, wantKeys, min(4, int(record.NumRows())))
			for _, batchSize := range []int{1, 2, 4, 64} {
				t.Run(strconv.Itoa(batchSize), func(t *testing.T) {
					batchCtx := internal.WithTableProperties(ctx, iceberg.Properties{
						internal.ParquetBatchSizeKey: strconv.Itoa(batchSize),
					})
					gotKeys, gotNames, err := readEqualityDeleteFile(batchCtx, iceio.LocalFS{}, schema, nil, dataFile, fieldIDs)
					require.NoError(t, err)
					assert.Equal(t, wantKeys, gotKeys)
					assert.Equal(t, wantNames, gotNames)
				})
			}
		})
	}
}

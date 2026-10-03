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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func BenchmarkReadEqualityDeleteFileBatchSizes(b *testing.B) {
	const numRows = 65_536
	for _, numColumns := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("columns=%d", numColumns), func(b *testing.B) {
			fields := make([]iceberg.NestedField, numColumns)
			fieldIDs := make([]int, numColumns)
			for i := range fields {
				fields[i] = iceberg.NestedField{ID: i + 1, Name: fmt.Sprintf("key_%d", i), Type: iceberg.PrimitiveTypes.Int64}
				fieldIDs[i] = i + 1
			}
			schema := iceberg.NewSchema(0, fields...)
			arrowSchema, err := SchemaToArrowSchema(schema, nil, true, false)
			require.NoError(b, err)
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			for col := range numColumns {
				values := builder.Field(col).(*array.Int64Builder)
				values.Reserve(numRows)
				for row := range numRows {
					values.Append(int64(row + col))
				}
			}
			record := builder.NewRecordBatch()
			builder.Release()
			defer record.Release()
			tbl := array.NewTableFromRecords(arrowSchema, []arrow.RecordBatch{record})
			defer tbl.Release()
			path := filepath.Join(b.TempDir(), "equality-delete.parquet")
			file, err := (iceio.LocalFS{}).Create(path)
			require.NoError(b, err)
			require.NoError(b, pqarrow.WriteTable(tbl, file, numRows,
				parquet.NewWriterProperties(parquet.WithStats(true)), pqarrow.DefaultWriterProps()))
			info, err := os.Stat(path)
			require.NoError(b, err)
			dataBuilder, err := iceberg.NewDataFileBuilder(*iceberg.UnpartitionedSpec, iceberg.EntryContentEqDeletes,
				path, iceberg.ParquetFile, nil, nil, nil, numRows, info.Size())
			require.NoError(b, err)
			dataBuilder.EqualityFieldIDs(fieldIDs)
			dataFile := dataBuilder.Build()
			for _, batchSize := range []int{64, 256, 1024, 8192} {
				b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
					ctx := internal.WithTableProperties(context.Background(), iceberg.Properties{
						internal.ParquetBatchSizeKey: strconv.Itoa(batchSize),
					})
					b.ReportAllocs()
					for b.Loop() {
						keys, _, err := readEqualityDeleteFile(ctx, iceio.LocalFS{}, schema, nil, dataFile, fieldIDs)
						if err != nil {
							b.Fatal(err)
						}
						if len(keys) != numRows {
							b.Fatalf("got %d keys, want %d", len(keys), numRows)
						}
					}
				})
			}
		})
	}
}

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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table

import (
    "testing"

    "github.com/apache/iceberg-go"
    "github.com/stretchr/testify/require"
)

func TestSortUniquePositionDeleteRows(t *testing.T) {
    rows := []positionDeleteRow{
        {path: "b.parquet", position: 4},
        {path: "a.parquet", position: 10},
        {path: "a.parquet", position: 1},
        {path: "b.parquet", position: 4},
        {path: "a.parquet", position: 1},
        {path: "b.parquet", position: 2},
    }
    got := sortUniquePositionDeleteRows(rows)
    require.Equal(t, []positionDeleteRow{
        {path: "a.parquet", position: 1},
        {path: "a.parquet", position: 10},
        {path: "b.parquet", position: 2},
        {path: "b.parquet", position: 4},
    }, got)
}

func TestRewritePositionDeleteFilesEmptySnapshot(t *testing.T) {
    schema := iceberg.NewSchema(0,
        iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
    )
    meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(), nil)
    require.NoError(t, err)
    tbl := New([]string{"db", "example"}, meta, "", nil, nil)
    updated, rewritten, err := tbl.RewritePositionDeleteFiles(t.Context(), PositionDeleteRewriteOptions{})
    require.NoError(t, err)
    require.Zero(t, rewritten)
    require.Equal(t, tbl.Metadata(), updated.Metadata())
}

func TestRewritePositionDeleteFilesRejectsV3(t *testing.T) {
    schema := iceberg.NewSchema(0,
        iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
    )
    meta, err := NewMetadata(schema, iceberg.UnpartitionedSpec, UnsortedSortOrder, t.TempDir(),
        iceberg.Properties{PropertyFormatVersion: "3"})
    require.NoError(t, err)
    tbl := New([]string{"db", "example"}, meta, "", nil, nil)
    _, _, err = tbl.RewritePositionDeleteFiles(t.Context(), PositionDeleteRewriteOptions{})
    require.ErrorIs(t, err, iceberg.ErrNotImplemented)
}

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
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestGroupPosDeletesByFilePathUnalignedChunks(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	pa := stringArray(mem, "file-a", "file-b")
	pb := stringArray(mem, "file-a", "file-c", "file-a")
	paths := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{pa, pb})
	defer func() { paths.Release(); pa.Release(); pb.Release() }()

	posA := int64Array(mem, 7)
	posBuilder := array.NewInt64Builder(mem)
	posBuilder.Append(8)
	posBuilder.AppendNull()
	posBuilder.Append(10)
	posB := posBuilder.NewInt64Array()
	posBuilder.Release()
	posC := int64Array(mem, 11)
	positions := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posA, posB, posC})
	defer func() { positions.Release(); posA.Release(); posB.Release(); posC.Release() }()

	grouped, err := groupPosDeletesByFilePath(ctx, paths, positions)
	require.NoError(t, err)
	defer releasePosDeletes(grouped)
	require.Len(t, grouped, 3)

	a := grouped["file-a"].Chunk(0).(*array.Int64)
	require.Equal(t, 3, a.Len())
	require.Equal(t, int64(7), a.Value(0))
	require.True(t, a.IsNull(1))
	require.Equal(t, int64(11), a.Value(2))
	require.Equal(t, []int64{8}, int64Values(grouped["file-b"]))
	require.Equal(t, []int64{10}, int64Values(grouped["file-c"]))
}

func TestGroupPosDeletesByFilePathEmptyAndInvalidPositions(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	ctx := compute.WithAllocator(t.Context(), mem)

	emptyPaths := arrow.NewChunked(arrow.BinaryTypes.String, nil)
	emptyPositions := arrow.NewChunked(arrow.PrimitiveTypes.Int64, nil)
	grouped, err := groupPosDeletesByFilePath(ctx, emptyPaths, emptyPositions)
	require.NoError(t, err)
	require.Empty(t, grouped)
	emptyPaths.Release()
	emptyPositions.Release()

	pathArray := stringArray(mem, "file-a", "file-b")
	pathCol := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{pathArray})
	defer func() { pathCol.Release(); pathArray.Release() }()

	shortArray := int64Array(mem, 1)
	shortCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{shortArray})
	_, err = groupPosDeletesByFilePath(ctx, pathCol, shortCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	shortCol.Release()
	shortArray.Release()

	invalidArray := int32Array(mem, 1, 2)
	invalidCol := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{invalidArray})
	_, err = groupPosDeletesByFilePath(ctx, pathCol, invalidCol)
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
	invalidCol.Release()
	invalidArray.Release()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	positionArray := int64Array(mem, 1, 2)
	positionCol := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{positionArray})
	_, err = groupPosDeletesByFilePath(canceled, pathCol, positionCol)
	require.ErrorIs(t, err, context.Canceled)
	positionCol.Release()
	positionArray.Release()
}

// referenceGroupPosDeletesByFilePath is the old implementation, retained only
// in benchmarks so baseline and optimized versions run in the same process.
// Unlike the optimized path it performs one full equality/filter pass per file.
func referenceGroupPosDeletesByFilePath(ctx context.Context, filePathCol, posCol *arrow.Chunked) (map[string]*arrow.Chunked, error) {
	input := compute.NewDatum(filePathCol)
	unique, err := compute.Unique(ctx, input)
	input.Release()
	if err != nil {
		return nil, err
	}
	uniquePaths := unique.(*compute.ArrayDatum).MakeArray()
	unique.Release()
	defer uniquePaths.Release()

	paths, err := filePathValues(uniquePaths)
	if err != nil {
		return nil, err
	}
	if err := validateFilePathValues(paths, uniquePaths); err != nil {
		return nil, err
	}

	results := make(map[string]*arrow.Chunked, uniquePaths.Len())
	for i := 0; i < uniquePaths.Len(); i++ {
		sc, err := scalar.GetScalar(uniquePaths, i)
		if err != nil {
			releasePosDeletes(results)
			return nil, err
		}
		scDatum := compute.NewDatum(sc)
		if releasable, ok := sc.(scalar.Releasable); ok {
			releasable.Release()
		}
		mask, err := compute.CallFunction(ctx, "equal", nil,
			compute.NewDatumWithoutOwning(filePathCol), scDatum)
		scDatum.Release()
		if err != nil {
			releasePosDeletes(results)
			return nil, err
		}

		filtered, err := compute.Filter(ctx, compute.NewDatumWithoutOwning(posCol),
			mask, *compute.DefaultFilterOptions())
		mask.Release()
		if err != nil {
			releasePosDeletes(results)
			return nil, err
		}
		filteredChunked, ok := filtered.(*compute.ChunkedDatum)
		if !ok {
			kind := filtered.Kind()
			filtered.Release()
			releasePosDeletes(results)
			return nil, fmt.Errorf("reference filter returned %s", kind)
		}
		filteredChunked.Value.Retain()
		results[paths.Value(i)] = filteredChunked.Value
		filtered.Release()
	}
	return results, nil
}

func BenchmarkGroupPosDeletesByFilePath(b *testing.B) {
	const rows = 32_768
	for _, files := range []int{1, 8, 64, 512, 2048} {
		b.Run(fmt.Sprintf("rows=%d/files=%d", rows, files), func(b *testing.B) {
			mem := memory.DefaultAllocator
			names := make([]string, files)
			for i := range names {
				names[i] = fmt.Sprintf("s3://bucket/partition=%04d/file.parquet", i)
			}
			pathBuilder := array.NewStringBuilder(mem)
			posBuilder := array.NewInt64Builder(mem)
			for i := 0; i < rows; i++ {
				pathBuilder.Append(names[i%files])
				posBuilder.Append(int64(i))
			}
			pathsArray := pathBuilder.NewStringArray()
			posArray := posBuilder.NewInt64Array()
			pathBuilder.Release()
			posBuilder.Release()
			paths := arrow.NewChunked(arrow.BinaryTypes.String, []arrow.Array{pathsArray})
			positions := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{posArray})
			b.Cleanup(func() {
				paths.Release()
				positions.Release()
				pathsArray.Release()
				posArray.Release()
			})

			for _, variant := range []struct {
				name  string
				group func(context.Context, *arrow.Chunked, *arrow.Chunked) (map[string]*arrow.Chunked, error)
			}{
				{"reference", referenceGroupPosDeletesByFilePath},
				{"one-pass", groupPosDeletesByFilePath},
			} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						grouped, err := variant.group(b.Context(), paths, positions)
						if err != nil {
							b.Fatal(err)
						}
						if len(grouped) != files {
							b.Fatalf("expected %d files, got %d", files, len(grouped))
						}
						releasePosDeletes(grouped)
					}
				})
			}
		})
	}
}

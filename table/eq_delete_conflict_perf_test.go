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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestEqDeleteConflictSingleFieldWithNullAndSeparatedValues(t *testing.T) {
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "region", Type: iceberg.PrimitiveTypes.String},
	)
	meta := partitionedConflictMeta(t, schema, 1, nil)
	spec := meta.PartitionSpec()
	fieldID := spec.Field(0).FieldID

	var files []iceberg.DataFile
	for i, value := range []any{"west", "east", nil, "central", "west"} {
		files = append(files, equalityDeleteTestFile(t, spec,
			fmt.Sprintf("delete-%d.parquet", i), map[int]any{fieldID: value}))
	}
	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)

	want := iceberg.NewOr(
		iceberg.SetPredicate(iceberg.OpIn, iceberg.Reference("region"),
			[]iceberg.Literal{
				iceberg.StringLiteral("west"),
				iceberg.StringLiteral("east"),
				iceberg.StringLiteral("central"),
			}),
		iceberg.IsNull(iceberg.Reference("region")),
	)
	require.True(t, filter.Equals(want), "single-field values must be grouped without dropping nulls")
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
}

func TestEqDeleteConflictLargeMultiFieldPredicateBinds(t *testing.T) {
	const fieldCount = 3
	fields := make([]iceberg.NestedField, fieldCount)
	partFields := make([]iceberg.PartitionField, fieldCount)
	for i := range fields {
		fields[i] = iceberg.NestedField{
			ID: i + 1, Name: fmt.Sprintf("field_%d", i), Type: iceberg.PrimitiveTypes.String,
		}
		partFields[i] = iceberg.PartitionField{
			FieldID: 1000 + i, SourceIDs: []int{i + 1},
			Name: fmt.Sprintf("field_%d", i), Transform: iceberg.IdentityTransform{},
		}
	}
	schema := iceberg.NewSchema(0, fields...)
	spec := iceberg.NewPartitionSpec(partFields...)
	meta, err := NewMetadata(schema, &spec, UnsortedSortOrder, "file:///tmp/conflict-balanced",
		iceberg.Properties{PropertyFormatVersion: "2"})
	require.NoError(t, err)

	files := make([]iceberg.DataFile, 256)
	for i := range files {
		p := make(map[int]any, fieldCount)
		for field := range fieldCount {
			p[1000+field] = fmt.Sprintf("v-%d-%d", i, field)
		}
		files[i] = equalityDeleteTestFile(t, spec, fmt.Sprintf("del-%d.parquet", i), p)
	}

	filter, err := eqDeletePartitionsToFilter(files, meta)
	require.NoError(t, err)
	require.Equal(t, iceberg.OpOr, filter.Op())
	_, err = iceberg.BindExpr(meta.CurrentSchema(), filter, true)
	require.NoError(t, err)
}

func TestBalancedConflictDisjunctionPreservesTerms(t *testing.T) {
	a := iceberg.EqualTo(iceberg.Reference("id"), int64(1))
	b := iceberg.EqualTo(iceberg.Reference("id"), int64(2))
	c := iceberg.EqualTo(iceberg.Reference("id"), int64(3))
	d := iceberg.EqualTo(iceberg.Reference("id"), int64(4))

	require.True(t, balancedConflictDisjunction(nil).Equals(iceberg.AlwaysTrue{}))
	require.True(t, balancedConflictDisjunction([]iceberg.BooleanExpression{a}).Equals(a))
	want := iceberg.NewOr(iceberg.NewOr(a, b), iceberg.NewOr(c, d))
	got := balancedConflictDisjunction([]iceberg.BooleanExpression{a, b, c, d})
	require.True(t, got.Equals(want))
}

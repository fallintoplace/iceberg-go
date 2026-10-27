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

package iceberg_test

import (
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompatibilityRestoresFieldLookupAcrossContainers(t *testing.T) {
	read := schemaOf(
		required(1, "nested", nestedStruct(required(2, "leaf", iceberg.PrimitiveTypes.Int32))),
		required(3, "sibling", iceberg.PrimitiveTypes.Int32),
		required(4, "items", &iceberg.ListType{
			ElementID: 5,
			Element:   nestedStruct(required(6, "leaf", iceberg.PrimitiveTypes.String)),
		}),
		required(7, "attrs", &iceberg.MapType{
			KeyID:     8,
			KeyType:   iceberg.PrimitiveTypes.String,
			ValueID:   9,
			ValueType: nestedStruct(required(10, "leaf", iceberg.PrimitiveTypes.Int32)),
		}),
	)
	write := schemaOf(
		required(1, "nested", nestedStruct(required(2, "leaf", iceberg.PrimitiveTypes.Int64))),
		required(3, "sibling", iceberg.PrimitiveTypes.Int64),
		required(4, "items", &iceberg.ListType{
			ElementID: 5,
			Element:   nestedStruct(required(6, "leaf", iceberg.PrimitiveTypes.Int32)),
		}),
		required(7, "attrs", &iceberg.MapType{
			KeyID:     8,
			KeyType:   iceberg.PrimitiveTypes.String,
			ValueID:   9,
			ValueType: nestedStruct(required(10, "leaf", iceberg.PrimitiveTypes.Int64)),
		}),
	)

	want := []string{
		"nested.leaf: long cannot be promoted to int",
		"sibling: long cannot be promoted to int",
		"items.leaf: int cannot be promoted to string",
		"attrs.leaf: long cannot be promoted to int",
	}

	checks := []struct {
		name  string
		check func(*iceberg.Schema, *iceberg.Schema) ([]string, error)
	}{
		{"read", iceberg.ReadCompatibilityErrors},
		{"write", func(read, write *iceberg.Schema) ([]string, error) {
			return iceberg.WriteCompatibilityErrors(read, write, true)
		}},
		{"type", func(read, write *iceberg.Schema) ([]string, error) {
			return iceberg.TypeCompatibilityErrors(read, write, true)
		}},
	}

	for _, tc := range checks {
		t.Run(tc.name, func(t *testing.T) {
			errs, err := tc.check(read, write)
			require.NoError(t, err)
			assert.Equal(t, want, errs)
		})
	}
}

func TestCompatibilityFieldOrderingWithMissingOptionalField(t *testing.T) {
	read := schemaOf(
		required(10, "first", iceberg.PrimitiveTypes.String),
		required(11, "second", iceberg.PrimitiveTypes.String),
		optional(99, "missing", iceberg.PrimitiveTypes.String),
		required(12, "third", iceberg.PrimitiveTypes.String),
	)
	write := schemaOf(
		required(12, "third", iceberg.PrimitiveTypes.String),
		required(10, "first", iceberg.PrimitiveTypes.String),
		required(11, "second", iceberg.PrimitiveTypes.String),
	)

	errs, err := iceberg.WriteCompatibilityErrors(read, write, true)
	require.NoError(t, err)
	assert.Equal(t, []string{"third is out of order, before second"}, errs)

	errs, err = iceberg.ReadCompatibilityErrors(read, write)
	require.NoError(t, err)
	assert.Empty(t, errs)
}

func BenchmarkCompatibilityWideSchema(b *testing.B) {
	for _, width := range []int{16, 128, 1024} {
		fields := make([]iceberg.NestedField, width)
		for i := range fields {
			fields[i] = required(i+1, fmt.Sprintf("column_%d", i), iceberg.PrimitiveTypes.Int32)
		}
		read, write := schemaOf(fields...), schemaOf(fields...)

		b.Run(fmt.Sprintf("%d_fields", width), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				errs, err := iceberg.WriteCompatibilityErrors(read, write, true)
				if err != nil || len(errs) != 0 {
					b.Fatalf("unexpected compatibility result: %v, %v", errs, err)
				}
			}
		})
	}
}

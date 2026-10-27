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
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestSchemaWithRowLineageColumnsSelection(t *testing.T) {
	base := iceberg.NewSchemaWithIdentifiers(7, []int{1},
		iceberg.NestedField{ID: 1, Name: "id", Required: true, Type: iceberg.PrimitiveTypes.Int64},
	)
	tests := []struct {
		name    string
		columns iceberg.RowLineageColumns
		wantIDs []int
	}{
		{name: "none", wantIDs: []int{1}},
		{name: "row ID only", columns: iceberg.RowLineageColumns{RowID: true}, wantIDs: []int{1, iceberg.RowIDFieldID}},
		{name: "sequence only", columns: iceberg.RowLineageColumns{LastUpdatedSequenceNumber: true}, wantIDs: []int{1, iceberg.LastUpdatedSequenceNumberFieldID}},
		{name: "both", columns: iceberg.RowLineageColumns{RowID: true, LastUpdatedSequenceNumber: true}, wantIDs: []int{1, iceberg.RowIDFieldID, iceberg.LastUpdatedSequenceNumberFieldID}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := iceberg.SchemaWithRowLineageColumns(base, tt.columns)
			if got == base {
				t.Fatal("expected a new schema")
			}
			if got.ID != base.ID || !slices.Equal(got.IdentifierFieldIDs, base.IdentifierFieldIDs) {
				t.Fatalf("schema identity changed: got ID %d, identifier fields %v", got.ID, got.IdentifierFieldIDs)
			}

			fields := got.Fields()
			gotIDs := make([]int, len(fields))
			for i, field := range fields {
				gotIDs[i] = field.ID
				if field.ID == iceberg.RowIDFieldID || field.ID == iceberg.LastUpdatedSequenceNumberFieldID {
					if field.Required || !field.Type.Equals(iceberg.PrimitiveTypes.Int64) {
						t.Errorf("lineage field %s should be optional long, got %#v", field.Name, field)
					}
				}
			}
			if !slices.Equal(gotIDs, tt.wantIDs) {
				t.Fatalf("field IDs = %v, want %v", gotIDs, tt.wantIDs)
			}
			if again := iceberg.SchemaWithRowLineageColumns(got, tt.columns); !again.Equals(got) {
				t.Fatal("applying selection twice changed the schema")
			}
		})
	}

	if base.NumFields() != 1 {
		t.Fatalf("source schema modified: %v", base.Fields())
	}
}

func TestSchemaWithRowLineageColumnsExistingFields(t *testing.T) {
	base := iceberg.NewSchema(3, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})
	withRowID := iceberg.SchemaWithRowID(base)
	got := iceberg.SchemaWithRowLineageColumns(withRowID, iceberg.RowLineageColumns{
		RowID: true, LastUpdatedSequenceNumber: true,
	})
	if got.NumFields() != 3 {
		t.Fatalf("expected existing row ID to be reused, got %d fields", got.NumFields())
	}
	if again := iceberg.SchemaWithRowLineageColumns(got, iceberg.RowLineageColumns{}); !again.Equals(got) {
		t.Fatal("empty options should preserve existing fields")
	}
	if lineage := iceberg.SchemaWithRowLineage(base); !lineage.Equals(got) {
		t.Fatal("SchemaWithRowLineage should include the same fields as both options")
	}
}

func TestSchemaWithRowLineageColumnsNilSchema(t *testing.T) {
	if got := iceberg.SchemaWithRowLineageColumns(nil, iceberg.RowLineageColumns{RowID: true}); got != nil {
		t.Fatalf("nil schema returned %v", got)
	}
}

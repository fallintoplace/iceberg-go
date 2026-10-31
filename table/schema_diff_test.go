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
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package table

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func schemaDiffTestTable(schemas ...*iceberg.Schema) Table {
	return Table{metadata: &metadataV2{commonMetadata: commonMetadata{SchemaList: schemas}}}
}

func schemaDiffTestField(id int, name string, typ iceberg.Type, required bool) iceberg.NestedField {
	return iceberg.NestedField{ID: id, Name: name, Type: typ, Required: required}
}

func schemaDiffTestChanges(t *testing.T, changes []SchemaFieldChange, id int) *SchemaFieldChange {
	t.Helper()
	for i := range changes {
		if changes[i].FieldID == id {
			return &changes[i]
		}
	}
	t.Fatalf("no change for field ID %d: %+v", id, changes)
	return nil
}

func TestDiffSchemasNestedEvolution(t *testing.T) {
	oldName := schemaDiffTestField(3, "name", iceberg.PrimitiveTypes.String, false)
	oldName.Doc = "original"
	oldName.InitialDefault = "unknown"
	oldName.WriteDefault = "anonymous"
	newName := schemaDiffTestField(3, "display_name", iceberg.PrimitiveTypes.Int64, true)
	newName.Doc = "renamed"
	newName.InitialDefault = int64(0)
	newName.WriteDefault = int64(1)

	before := iceberg.NewSchemaWithIdentifiers(1, []int{1, 3},
		schemaDiffTestField(1, "id", iceberg.PrimitiveTypes.Int64, true),
		schemaDiffTestField(2, "person", &iceberg.StructType{FieldList: []iceberg.NestedField{
			oldName,
			schemaDiffTestField(14, "moved", iceberg.PrimitiveTypes.Int32, false),
		}}, false),
		schemaDiffTestField(4, "items", &iceberg.ListType{
			ElementID: 5, ElementRequired: false,
			Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				schemaDiffTestField(6, "value", iceberg.PrimitiveTypes.Int32, false),
			}},
		}, false),
		schemaDiffTestField(8, "tags", &iceberg.MapType{
			KeyID: 9, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 10, ValueType: iceberg.PrimitiveTypes.String,
		}, false),
		schemaDiffTestField(11, "removed", iceberg.PrimitiveTypes.String, false),
	)
	// The nested field ID 14 moves to the table root. New field 12 becomes
	// an identifier, and existing identifiers are independently compared.
	after := iceberg.NewSchemaWithIdentifiers(2, []int{3, 12},
		schemaDiffTestField(1, "id", iceberg.PrimitiveTypes.Int64, true),
		schemaDiffTestField(8, "tags", &iceberg.MapType{
			KeyID: 9, KeyType: iceberg.PrimitiveTypes.String,
			ValueID: 10, ValueType: iceberg.PrimitiveTypes.Int64,
		}, false),
		schemaDiffTestField(2, "profile", &iceberg.StructType{FieldList: []iceberg.NestedField{newName}}, false),
		schemaDiffTestField(12, "new", iceberg.PrimitiveTypes.String, true),
		schemaDiffTestField(4, "items", &iceberg.ListType{
			ElementID: 5, ElementRequired: true,
			Element: &iceberg.StructType{FieldList: []iceberg.NestedField{
				schemaDiffTestField(6, "value", iceberg.PrimitiveTypes.Int32, false),
			}},
		}, false),
		schemaDiffTestField(14, "moved", iceberg.PrimitiveTypes.Int32, false),
	)

	tbl := schemaDiffTestTable(before, after)
	got, err := tbl.DiffSchemas(1, 2)
	require.NoError(t, err)
	require.Equal(t, 1, got.FromSchemaID)
	require.Equal(t, 2, got.ToSchemaID)
	require.Equal(t, []int{12}, got.AddedIdentifierFieldIDs)
	require.Equal(t, []int{1}, got.RemovedIdentifierFieldIDs)

	var changedIDs []int
	for _, change := range got.Changes {
		changedIDs = append(changedIDs, change.FieldID)
	}
	require.Equal(t, []int{2, 3, 4, 5, 8, 10, 11, 12, 14}, changedIDs)
	require.Equal(t, []SchemaChangeKind{SchemaChangeRenamed, SchemaChangeReordered},
		schemaDiffTestChanges(t, got.Changes, 2).Changes)
	require.Equal(t, []SchemaChangeKind{
		SchemaChangeRenamed, SchemaChangeType, SchemaChangeRequiredness,
		SchemaChangeDocumentation, SchemaChangeInitialDefault, SchemaChangeWriteDefault,
	}, schemaDiffTestChanges(t, got.Changes, 3).Changes)
	require.Equal(t, []string{"person", "name"}, schemaDiffTestChanges(t, got.Changes, 3).Before.Path)
	require.Equal(t, []string{"profile", "display_name"}, schemaDiffTestChanges(t, got.Changes, 3).After.Path)
	require.Equal(t, []SchemaChangeKind{SchemaChangeReordered}, schemaDiffTestChanges(t, got.Changes, 4).Changes)
	require.Equal(t, []SchemaChangeKind{SchemaChangeRequiredness}, schemaDiffTestChanges(t, got.Changes, 5).Changes)
	require.Equal(t, []SchemaChangeKind{SchemaChangeReordered}, schemaDiffTestChanges(t, got.Changes, 8).Changes)
	require.Equal(t, []SchemaChangeKind{SchemaChangeType}, schemaDiffTestChanges(t, got.Changes, 10).Changes)
	require.Equal(t, []SchemaChangeKind{SchemaChangeRemoved}, schemaDiffTestChanges(t, got.Changes, 11).Changes)
	require.Nil(t, schemaDiffTestChanges(t, got.Changes, 11).After)
	require.Equal(t, []SchemaChangeKind{SchemaChangeAdded}, schemaDiffTestChanges(t, got.Changes, 12).Changes)
	require.Nil(t, schemaDiffTestChanges(t, got.Changes, 12).Before)
	require.Equal(t, []SchemaChangeKind{SchemaChangeMoved}, schemaDiffTestChanges(t, got.Changes, 14).Changes)
	require.Equal(t, 2, schemaDiffTestChanges(t, got.Changes, 14).Before.ParentID)
	require.Equal(t, -1, schemaDiffTestChanges(t, got.Changes, 14).After.ParentID)

	// No bogus changes to the child's type when only an ancestor is renamed.
	// Matching the list/map synthetic children by ID avoids cascading type
	// changes through a parent container.
	for _, change := range got.Changes {
		require.NotEqual(t, 6, change.FieldID)
		require.NotEqual(t, 9, change.FieldID)
	}
	// Defensive schema copies also keep returned diff paths independent.
	schemaDiffTestChanges(t, got.Changes, 3).After.Path[0] = "tampered"
	again, err := tbl.DiffSchemas(1, 2)
	require.NoError(t, err)
	require.Equal(t, "profile", schemaDiffTestChanges(t, again.Changes, 3).After.Path[0])
}

func TestDiffSchemasSiblingInsertDeleteDoesNotCountAsReorder(t *testing.T) {
	old := iceberg.NewSchema(1,
		schemaDiffTestField(1, "a", iceberg.PrimitiveTypes.String, false),
		schemaDiffTestField(2, "b", iceberg.PrimitiveTypes.String, false),
		schemaDiffTestField(3, "c", iceberg.PrimitiveTypes.String, false),
	)
	next := iceberg.NewSchema(2,
		schemaDiffTestField(4, "new", iceberg.PrimitiveTypes.String, false),
		schemaDiffTestField(2, "b", iceberg.PrimitiveTypes.String, false),
		schemaDiffTestField(3, "c", iceberg.PrimitiveTypes.String, false),
	)
	got, err := schemaDiffTestTable(old, next).DiffSchemas(1, 2)
	require.NoError(t, err)
	require.Len(t, got.Changes, 2)
	require.Equal(t, 1, got.Changes[0].FieldID)
	require.Equal(t, []SchemaChangeKind{SchemaChangeRemoved}, got.Changes[0].Changes)
	require.Equal(t, 4, got.Changes[1].FieldID)
	require.Equal(t, []SchemaChangeKind{SchemaChangeAdded}, got.Changes[1].Changes)
}

func TestDiffSchemasNestedPathsWithDotsAndMapChildren(t *testing.T) {
	before := iceberg.NewSchema(1, schemaDiffTestField(1, "a.b", &iceberg.MapType{
		KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 3, ValueType: iceberg.PrimitiveTypes.String,
	}, false))
	after := iceberg.NewSchema(2, schemaDiffTestField(1, "a.b", &iceberg.MapType{
		KeyID: 2, KeyType: iceberg.PrimitiveTypes.String,
		ValueID: 3, ValueType: iceberg.PrimitiveTypes.Int64,
	}, false))
	got, err := schemaDiffTestTable(before, after).DiffSchemas(1, 2)
	require.NoError(t, err)
	require.Len(t, got.Changes, 1)
	require.Equal(t, 3, got.Changes[0].FieldID)
	require.Equal(t, []string{"a.b", "value"}, got.Changes[0].Before.Path)
	require.Equal(t, []SchemaChangeKind{SchemaChangeType}, got.Changes[0].Changes)
}

func TestDiffSchemasIdentifierChangesOnlyAndUnchangedSchemas(t *testing.T) {
	old := iceberg.NewSchemaWithIdentifiers(1, []int{1},
		schemaDiffTestField(1, "id", iceberg.PrimitiveTypes.Int64, true),
		schemaDiffTestField(2, "name", iceberg.PrimitiveTypes.String, true))
	next := iceberg.NewSchemaWithIdentifiers(2, []int{2},
		schemaDiffTestField(1, "id", iceberg.PrimitiveTypes.Int64, true),
		schemaDiffTestField(2, "name", iceberg.PrimitiveTypes.String, true))
	tbl := schemaDiffTestTable(old, next)
	got, err := tbl.DiffSchemas(1, 2)
	require.NoError(t, err)
	require.Empty(t, got.Changes)
	require.Equal(t, []int{2}, got.AddedIdentifierFieldIDs)
	require.Equal(t, []int{1}, got.RemovedIdentifierFieldIDs)

	same, err := tbl.DiffSchemas(2, 2)
	require.NoError(t, err)
	require.Equal(t, 2, same.FromSchemaID)
	require.Equal(t, 2, same.ToSchemaID)
	require.Empty(t, same.Changes)
	require.Empty(t, same.AddedIdentifierFieldIDs)
	require.Empty(t, same.RemovedIdentifierFieldIDs)
}

func TestDiffSchemasErrors(t *testing.T) {
	_, err := (Table{}).DiffSchemas(1, 2)
	require.ErrorIs(t, err, ErrInvalidMetadata)

	tbl := schemaDiffTestTable(iceberg.NewSchema(1, schemaDiffTestField(1, "id", iceberg.PrimitiveTypes.Int64, true)))
	_, err = tbl.DiffSchemas(0, 1)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "starting schema ID 0")
	_, err = tbl.DiffSchemas(1, 2)
	require.ErrorIs(t, err, iceberg.ErrInvalidArgument)
	require.ErrorContains(t, err, "ending schema ID 2")

	// The indexer rejects malformed schemas without silently overwriting IDs.
	bad := &iceberg.Schema{}
	_, _, err = schemaDiffIndex(bad)
	require.NoError(t, err)
	_, _, err = schemaDiffIndex(iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "bad"}))
	require.ErrorIs(t, err, iceberg.ErrInvalidSchema)
}

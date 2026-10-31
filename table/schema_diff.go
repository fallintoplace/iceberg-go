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
	"fmt"
	"reflect"
	"slices"

	"github.com/apache/iceberg-go"
)

// SchemaChangeKind identifies one difference for a field matched by Iceberg
// field ID. A field can have several changes (such as a rename and a type
// promotion) recorded in a single SchemaFieldChange.
type SchemaChangeKind string

const (
	SchemaChangeAdded          SchemaChangeKind = "added"
	SchemaChangeRemoved        SchemaChangeKind = "removed"
	SchemaChangeRenamed        SchemaChangeKind = "renamed"
	SchemaChangeMoved          SchemaChangeKind = "moved"
	SchemaChangeReordered      SchemaChangeKind = "reordered"
	SchemaChangeType           SchemaChangeKind = "type"
	SchemaChangeRequiredness   SchemaChangeKind = "requiredness"
	SchemaChangeDocumentation  SchemaChangeKind = "documentation"
	SchemaChangeInitialDefault SchemaChangeKind = "initial-default"
	SchemaChangeWriteDefault   SchemaChangeKind = "write-default"
)

// SchemaFieldState describes a field at one point in schema history. Path is
// expressed as segments, not a dot-separated string, so names containing dots
// remain unambiguous. Struct/list/map children use the Iceberg path segments
// "element", "key", and "value". Type is the field's primitive type (including
// fixed length or decimal precision) or the shallow container kind.
//
// ParentID is -1 for root fields. Position is zero-based among siblings.
// Default values are intentionally not returned because they may contain
// mutable application data; the diff reports whether each default changed.
type SchemaFieldState struct {
	Name     string
	Path     []string
	Type     string
	ParentID int
	Position int
	Required bool
	Doc      string
}

// SchemaFieldChange reports differences for exactly one stable Iceberg field
// ID. Before is nil for additions, and After is nil for removals. Changes are
// in a fixed order independent of map iteration or schema traversal.
type SchemaFieldChange struct {
	FieldID int
	Changes []SchemaChangeKind
	Before  *SchemaFieldState
	After   *SchemaFieldState
}

// SchemaDiff is a metadata-only comparison of two historical table schemas.
// Changes are ordered by field ID. Added/RemovedIdentifierFieldIDs contain
// sorted ID sets and are separate from physical field changes.
//
// This is a description of differences, not an automatic compatibility or
// migration verdict. A rename, move, or type change is reported but not
// classified as safe to apply to a given table format version.
type SchemaDiff struct {
	FromSchemaID int
	ToSchemaID   int

	Changes                   []SchemaFieldChange
	AddedIdentifierFieldIDs   []int
	RemovedIdentifierFieldIDs []int
}

// DiffSchemas compares historical schemas by field ID, not field name. It
// handles recursively nested struct fields and synthetic list/map children,
// including moves and sibling reorders. Added or dropped siblings do not
// cause spurious reorder reports for unchanged fields. Renaming an ancestor
// does not report a rename of all its descendants.
//
// Both schema IDs must exist in the table metadata. The comparison is read-
// only and does not require a catalog or FileIO. It does not modify the table.
func (t Table) DiffSchemas(fromSchemaID, toSchemaID int) (SchemaDiff, error) {
	var result SchemaDiff
	if t.metadata == nil {
		return result, fmt.Errorf("%w: table metadata is not available", ErrInvalidMetadata)
	}
	before, after := schemaDiffLookupSchemas(t.metadata, fromSchemaID, toSchemaID)
	if before == nil {
		return result, fmt.Errorf("%w: starting schema ID %d is not in table metadata", iceberg.ErrInvalidArgument, fromSchemaID)
	}
	if after == nil {
		return result, fmt.Errorf("%w: ending schema ID %d is not in table metadata", iceberg.ErrInvalidArgument, toSchemaID)
	}

	result.FromSchemaID = fromSchemaID
	result.ToSchemaID = toSchemaID
	if fromSchemaID == toSchemaID {
		return result, nil
	}

	beforeFields, beforeSiblings, err := schemaDiffIndex(before)
	if err != nil {
		return SchemaDiff{}, fmt.Errorf("index starting schema %d: %w", fromSchemaID, err)
	}
	afterFields, afterSiblings, err := schemaDiffIndex(after)
	if err != nil {
		return SchemaDiff{}, fmt.Errorf("index ending schema %d: %w", toSchemaID, err)
	}
	beforePositions, afterPositions := schemaDiffCommonPositions(beforeFields, afterFields, beforeSiblings, afterSiblings)

	ids := make([]int, 0, len(beforeFields)+len(afterFields))
	for id := range beforeFields {
		ids = append(ids, id)
	}
	for id := range afterFields {
		if _, ok := beforeFields[id]; !ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		old, hadBefore := beforeFields[id]
		next, hasAfter := afterFields[id]
		change := SchemaFieldChange{FieldID: id}
		if hadBefore {
			previous := old.state
			change.Before = &previous
		}
		if hasAfter {
			current := next.state
			change.After = &current
		}
		switch {
		case !hadBefore:
			change.Changes = append(change.Changes, SchemaChangeAdded)
		case !hasAfter:
			change.Changes = append(change.Changes, SchemaChangeRemoved)
		default:
			if old.state.Name != next.state.Name {
				change.Changes = append(change.Changes, SchemaChangeRenamed)
			}
			if old.state.ParentID != next.state.ParentID {
				change.Changes = append(change.Changes, SchemaChangeMoved)
			} else if beforePositions[id] != afterPositions[id] {
				change.Changes = append(change.Changes, SchemaChangeReordered)
			}
			if old.state.Type != next.state.Type {
				change.Changes = append(change.Changes, SchemaChangeType)
			}
			if old.state.Required != next.state.Required {
				change.Changes = append(change.Changes, SchemaChangeRequiredness)
			}
			if old.state.Doc != next.state.Doc {
				change.Changes = append(change.Changes, SchemaChangeDocumentation)
			}
			if !reflect.DeepEqual(old.field.InitialDefault, next.field.InitialDefault) {
				change.Changes = append(change.Changes, SchemaChangeInitialDefault)
			}
			if !reflect.DeepEqual(old.field.WriteDefault, next.field.WriteDefault) {
				change.Changes = append(change.Changes, SchemaChangeWriteDefault)
			}
		}
		if len(change.Changes) > 0 {
			result.Changes = append(result.Changes, change)
		}
	}
	result.AddedIdentifierFieldIDs, result.RemovedIdentifierFieldIDs = schemaDiffIdentifiers(
		before.IdentifierFieldIDs, after.IdentifierFieldIDs)

	return result, nil
}

// Built-in metadata exposes a fast defensive-copy lookup. Keep a public
// Schemas() fallback for custom Metadata implementations.
func schemaDiffLookupSchemas(meta Metadata, fromID, toID int) (*iceberg.Schema, *iceberg.Schema) {
	if indexed, ok := meta.(interface{ schemaByID(int) *iceberg.Schema }); ok {
		return indexed.schemaByID(fromID), indexed.schemaByID(toID)
	}
	var before, after *iceberg.Schema
	for _, schema := range meta.Schemas() {
		if schema == nil {
			continue
		}
		if schema.ID == fromID {
			before = schema
		}
		if schema.ID == toID {
			after = schema
		}
	}
	return before, after
}

type schemaDiffNode struct {
	state SchemaFieldState
	field iceberg.NestedField
}

// schemaDiffIndex traverses a schema exactly once. It preserves child order
// and uses stable parent IDs, rather than string paths, to detect moves.
func schemaDiffIndex(schema *iceberg.Schema) (map[int]schemaDiffNode, map[int][]int, error) {
	nodes := make(map[int]schemaDiffNode)
	siblings := make(map[int][]int)
	var visit func(parentID int, path []string, fields []iceberg.NestedField) error
	visit = func(parentID int, path []string, fields []iceberg.NestedField) error {
		for position, field := range fields {
			if field.Type == nil {
				return fmt.Errorf("%w: field %d has no type", iceberg.ErrInvalidSchema, field.ID)
			}
			if _, exists := nodes[field.ID]; exists {
				return fmt.Errorf("%w: duplicate field ID %d", iceberg.ErrInvalidSchema, field.ID)
			}
			fieldPath := make([]string, len(path)+1)
			copy(fieldPath, path)
			fieldPath[len(path)] = field.Name
			nodes[field.ID] = schemaDiffNode{
				state: SchemaFieldState{
					Name: field.Name, Path: fieldPath, Type: schemaDiffType(field.Type),
					ParentID: parentID, Position: position, Required: field.Required, Doc: field.Doc,
				},
				field: field,
			}
			siblings[parentID] = append(siblings[parentID], field.ID)
			if nested, ok := field.Type.(iceberg.NestedType); ok {
				if err := visit(field.ID, fieldPath, nested.Fields()); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := visit(-1, nil, schema.Fields()); err != nil {
		return nil, nil, err
	}
	return nodes, siblings, nil
}

// A parent struct changes deeply when its child changes, but only the child
// should get a type-change event. List/map element/key/value types have stable
// synthetic field IDs and are compared independently in the same manner.
func schemaDiffType(t iceberg.Type) string {
	switch t.(type) {
	case *iceberg.StructType, *iceberg.ListType, *iceberg.MapType:
		return t.Type()
	default:
		return t.String()
	}
}

// Normalize sibling indexes using only fields present under the SAME parent in
// both versions. A drop/add causes no false reorder, and a move reports just
// a move rather than also reporting a reorder of unrelated siblings.
func schemaDiffCommonPositions(before, after map[int]schemaDiffNode, beforeSiblings, afterSiblings map[int][]int) (map[int]int, map[int]int) {
	oldPositions := make(map[int]int)
	newPositions := make(map[int]int)
	for parent, ids := range beforeSiblings {
		position := 0
		for _, id := range ids {
			if counterpart, ok := after[id]; ok && counterpart.state.ParentID == parent {
				oldPositions[id] = position
				position++
			}
		}
	}
	for parent, ids := range afterSiblings {
		position := 0
		for _, id := range ids {
			if counterpart, ok := before[id]; ok && counterpart.state.ParentID == parent {
				newPositions[id] = position
				position++
			}
		}
	}
	return oldPositions, newPositions
}

func schemaDiffIdentifiers(before, after []int) (added, removed []int) {
	beforeSet := make(map[int]struct{}, len(before))
	afterSet := make(map[int]struct{}, len(after))
	for _, id := range before {
		beforeSet[id] = struct{}{}
	}
	for _, id := range after {
		afterSet[id] = struct{}{}
	}
	for id := range afterSet {
		if _, ok := beforeSet[id]; !ok {
			added = append(added, id)
		}
	}
	for id := range beforeSet {
		if _, ok := afterSet[id]; !ok {
			removed = append(removed, id)
		}
	}
	slices.Sort(added)
	slices.Sort(removed)
	return added, removed
}

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
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	iceinternal "github.com/apache/iceberg-go/internal"
)

// flatReadProjection reuses flat, correctly ordered Arrow columns without
// walking the Iceberg schema for every record batch.
type flatReadProjection struct {
	target   *arrow.Schema
	fieldIDs []string
}

// A fast projection is possible only if every requested field is physically
// present in the same order with the same Iceberg ID and type. Nested fields,
// schema evolution, and column casts keep the normal schema visitor.
func newFlatReadProjection(
	requested, fileSchema *iceberg.Schema,
	useLargeTypes bool,
	properties iceberg.Properties,
) (*flatReadProjection, error) {
	if requested == nil || fileSchema == nil || requested.NumFields() == 0 ||
		requested.NumFields() != fileSchema.NumFields() {
		return nil, nil
	}

	requestedFields := requested.FieldsRef(iceinternal.SchemaRef{})
	fileFields := fileSchema.FieldsRef(iceinternal.SchemaRef{})
	ids := make([]string, len(requestedFields))
	for i, field := range requestedFields {
		physical := fileFields[i]
		if field.ID != physical.ID || field.Name != physical.Name ||
			field.Type == nil || physical.Type == nil || !field.Type.Equals(physical.Type) {
			return nil, nil
		}
		if _, ok := field.Type.(iceberg.PrimitiveType); !ok {
			return nil, nil
		}

		// Geospatial arrays may need extension metadata and rewrapping.
		switch field.Type.(type) {
		case iceberg.GeometryType, iceberg.GeographyType:
			return nil, nil
		}
		ids[i] = strconv.Itoa(field.ID)
	}

	target, err := SchemaToArrowSchemaWithOptions(requested, ArrowSchemaOptions{
		UseLargeTypes:   useLargeTypes,
		TableProperties: properties,
	})
	if err != nil {
		// The shortcut must not introduce a new setup error.
		return nil, nil
	}

	return &flatReadProjection{target: target, fieldIDs: ids}, nil
}

// project returns false when the record needs the normal projection. Unlike
// the full visitor, this only has to normalize field metadata on matching
// batches, so the value arrays can be shared without casting or copying.
func (p *flatReadProjection) project(batch arrow.RecordBatch) (arrow.RecordBatch, bool) {
	if p == nil || int(batch.NumCols()) != len(p.fieldIDs) {
		return nil, false
	}

	source := batch.Schema()
	for i, id := range p.fieldIDs {
		field, want := source.Field(i), p.target.Field(i)
		if field.Name != want.Name || field.Nullable != want.Nullable ||
			!arrow.TypeEqual(field.Type, want.Type) {
			return nil, false
		}
		if gotID, hasID := field.Metadata.GetValue(ArrowParquetFieldIDKey); hasID && gotID != id {
			return nil, false
		}
	}

	if arrowSchemaEqual(source, p.target) {
		batch.Retain()

		return batch, true
	}

	return array.NewRecordBatch(p.target, batch.Columns(), batch.NumRows()), true
}

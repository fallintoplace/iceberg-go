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
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

func arrowSchemaEqual(left, right *arrow.Schema) bool {
	if left == nil || right == nil {
		return left == right
	}

	// Schema.Equal ignores top-level metadata. Full projection strips it, so
	// guard it here; otherwise the fast path would preserve metadata projection drops.
	return left.Equal(right) && left.Metadata().Equal(right.Metadata())
}

type schemaCompatVisitor struct {
	provided *iceberg.Schema

	errorData [][]string
}

func checkSchemaCompat(requested, provided *iceberg.Schema) error {
	sc := &schemaCompatVisitor{
		provided:  provided,
		errorData: [][]string{{"", "Table Field", "Requested Field"}},
	}

	_, compat := iceberg.PreOrderVisit(requested, sc)

	return compat
}

func checkArrowSchemaCompat(requested *iceberg.Schema, provided *arrow.Schema, downcastNanoToMicro bool) error {
	mapping := requested.NameMapping()
	providedSchema, err := ArrowSchemaToIceberg(provided, downcastNanoToMicro, mapping)
	if err != nil {
		return err
	}

	return checkSchemaCompat(requested, providedSchema)
}

func checkArrowSchemaCompatWithProperties(requested *iceberg.Schema, provided *arrow.Schema, downcastNanoToMicro bool, props iceberg.Properties) error {
	providedSchema, err := ArrowSchemaToIcebergWithOptions(provided, ArrowToIcebergOptions{
		DowncastNsTimestamp: downcastNanoToMicro,
		NameMapping:         requested.NameMapping(),
		TableSchema:         requested,
		TableProperties:     props,
	})
	if err != nil {
		return err
	}

	return checkSchemaCompat(requested, providedSchema)
}

func (sc *schemaCompatVisitor) isFieldCompat(lhs iceberg.NestedField) bool {
	rhs, ok := sc.provided.FindFieldByID(lhs.ID)
	if !ok {
		if lhs.Required && lhs.WriteDefault == nil && lhs.InitialDefault == nil {
			sc.errorData = append(sc.errorData,
				[]string{"❌", lhs.String(), "missing"})

			return false
		}
		sc.errorData = append(sc.errorData,
			[]string{"✅", lhs.String(), "missing"})

		return true
	}

	if lhs.Required && !rhs.Required {
		sc.errorData = append(sc.errorData,
			[]string{"❌", lhs.String(), rhs.String()})

		return false
	}

	if lhs.Type.Equals(rhs.Type) {
		sc.errorData = append(sc.errorData,
			[]string{"✅", lhs.String(), rhs.String()})

		return true
	}

	// we only check that parent node is also of the same type
	// we check the type of the child nodes as we traverse them later
	switch lhs.Type.(type) {
	case *iceberg.StructType:
		if rhs, ok := rhs.Type.(*iceberg.StructType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	case *iceberg.ListType:
		if rhs, ok := rhs.Type.(*iceberg.ListType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	case *iceberg.MapType:
		if rhs, ok := rhs.Type.(*iceberg.MapType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	}

	if _, err := iceberg.PromoteType(rhs.Type, lhs.Type); err != nil {
		sc.errorData = append(sc.errorData,
			[]string{"❌", lhs.String(), rhs.String()})

		return false
	}

	sc.errorData = append(sc.errorData,
		[]string{"✅", lhs.String(), rhs.String()})

	return true
}

func (sc *schemaCompatVisitor) Schema(s *iceberg.Schema, v func() bool) bool {
	if !v() {
		// Render the collected mismatch rows into the error message with the
		// stdlib tabwriter. Unlike the former pterm renderer this writes only to
		// the returned error and has no stdout side effect. tabwriter measures
		// column width by rune count, so the leading status column (✅/❌) may be
		// off by one display cell in terminals that render those double-width;
		// the reported content is unaffected.
		var buf strings.Builder
		w := tabwriter.NewWriter(&buf, 0, 0, 2, ' ', 0)
		for _, row := range sc.errorData {
			fmt.Fprintln(w, strings.Join(row, "\t"))
		}
		_ = w.Flush()

		panic("mismatch in fields:\n" + buf.String())
	}

	return true
}

func (sc *schemaCompatVisitor) Struct(st iceberg.StructType, v []func() bool) bool {
	out := true
	for _, res := range v {
		out = res() && out
	}

	return out
}

func (sc *schemaCompatVisitor) Field(n iceberg.NestedField, v func() bool) bool {
	return sc.isFieldCompat(n) && v()
}

func (sc *schemaCompatVisitor) List(l iceberg.ListType, v func() bool) bool {
	return sc.isFieldCompat(l.ElementField()) && v()
}

func (sc *schemaCompatVisitor) Map(m iceberg.MapType, vk, vv func() bool) bool {
	return sc.isFieldCompat(m.KeyField()) && sc.isFieldCompat(m.ValueField()) && vk() && vv()
}

func (sc *schemaCompatVisitor) Primitive(p iceberg.PrimitiveType) bool {
	return true
}

func (sc *schemaCompatVisitor) Variant(v iceberg.VariantType) bool {
	return true
}

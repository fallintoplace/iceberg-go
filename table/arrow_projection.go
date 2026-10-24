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
	"encoding/json"
	"fmt"
	"slices"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/bitutil"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	"github.com/google/uuid"
)

type arrowAccessor struct {
	fileSchema *iceberg.Schema
}

func (a arrowAccessor) SchemaPartner(partner arrow.Array) arrow.Array {
	return partner
}

func (a arrowAccessor) FieldPartner(partnerStruct arrow.Array, fieldID int, _ string) arrow.Array {
	if partnerStruct == nil {
		return nil
	}

	field, ok := a.fileSchema.FindFieldByIDRef(fieldID, internal.SchemaRef{})
	if !ok {
		return nil
	}

	if st, ok := partnerStruct.(*array.Struct); ok {
		if idx, ok := st.DataType().(*arrow.StructType).FieldIdx(field.Name); ok {
			return st.Field(idx)
		}
	}

	panic(fmt.Errorf("cannot find %s in expected partner_struct type %s",
		field.Name, partnerStruct.DataType()))
}

func (a arrowAccessor) ListElementPartner(partnerList arrow.Array) arrow.Array {
	if l, ok := partnerList.(array.ListLike); ok {
		return l.ListValues()
	}

	return nil
}

func (a arrowAccessor) MapKeyPartner(partnerMap arrow.Array) arrow.Array {
	if m, ok := partnerMap.(*array.Map); ok {
		return m.Keys()
	}

	return nil
}

func (a arrowAccessor) MapValuePartner(partnerMap arrow.Array) arrow.Array {
	if m, ok := partnerMap.(*array.Map); ok {
		return m.Items()
	}

	return nil
}

func retOrPanic[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

// numericDefault converts v to T, accepting the typed iceberg form, the
// float64 that encoding/json produces when deserializing into any, or the
// json.Number that a decoder configured with UseNumber() produces.
func numericDefault[T ~int32 | ~int64 | ~float32 | ~float64](v any) T {
	switch val := v.(type) {
	case T:
		return val
	case float64:
		return T(val)
	case json.Number:
		f, err := val.Float64()
		if err != nil {
			panic(fmt.Errorf("unsupported json.Number %q for numeric iceberg type: %w", val, err))
		}

		return T(f)
	}
	panic(fmt.Errorf("unsupported write-default value type %T for numeric iceberg type", v))
}

// defaultToScalar converts an Iceberg default value to an Arrow scalar.
func defaultToScalar(v any, t iceberg.Type, dt arrow.DataType) scalar.Scalar {
	switch typ := t.(type) {
	case iceberg.Float32Type:
		s, err := scalar.MakeScalarParam(numericDefault[float32](v), dt)
		if err != nil {
			panic(fmt.Errorf("write-default float32 (iceberg type %s, value %v %T): %w", t, v, v, err))
		}

		return s
	case iceberg.DateType:
		return scalar.NewDate32Scalar(arrow.Date32(numericDefault[iceberg.Date](v)))
	case iceberg.TimeType:
		return scalar.NewTime64Scalar(arrow.Time64(numericDefault[iceberg.Time](v)), dt)
	case iceberg.TimestampType, iceberg.TimestampTzType:
		return scalar.NewTimestampScalar(arrow.Timestamp(numericDefault[iceberg.Timestamp](v)), dt)
	case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
		return scalar.NewTimestampScalar(arrow.Timestamp(numericDefault[iceberg.TimestampNano](v)), dt)
	case iceberg.UUIDType:
		switch val := v.(type) {
		case uuid.UUID:
			s, err := scalar.MakeScalarParam(val[:], &arrow.FixedSizeBinaryType{ByteWidth: 16})
			if err != nil {
				panic(fmt.Errorf("write-default uuid (value %v): %w", val, err))
			}

			return s
		case string:
			u, err := uuid.Parse(val)
			if err != nil {
				panic(fmt.Errorf("write-default uuid: cannot parse string %q: %w", val, err))
			}
			s, err := scalar.MakeScalarParam(u[:], &arrow.FixedSizeBinaryType{ByteWidth: 16})
			if err != nil {
				panic(fmt.Errorf("write-default uuid (value %v): %w", val, err))
			}

			return s
		}
		panic(fmt.Errorf("write-default uuid: unsupported value type %T (%v)", v, v))
	case iceberg.DecimalType:
		switch val := v.(type) {
		case iceberg.Decimal:
			return scalar.NewDecimal128Scalar(val.Val, dt)
		case string:
			n, err := decimal128.FromString(val, int32(typ.Precision()), int32(typ.Scale()))
			if err != nil {
				panic(fmt.Errorf("write-default decimal(p=%d, s=%d): cannot parse string %q: %w", typ.Precision(), typ.Scale(), val, err))
			}

			return scalar.NewDecimal128Scalar(n, dt)
		}
		panic(fmt.Errorf("write-default decimal: unsupported value type %T (%v)", v, v))
	case iceberg.BinaryType, iceberg.FixedType:
		switch val := v.(type) {
		case []byte:
			s, err := scalar.MakeScalarParam(val, dt)
			if err != nil {
				panic(fmt.Errorf("write-default binary/fixed (iceberg type %s, value %v): %w", t, val, err))
			}

			return s
		case string:
			fixedLen := -1
			if fixed, ok := t.(iceberg.FixedType); ok {
				fixedLen = fixed.Len()
			}
			b, err := internal.DecodeDefaultBytes(val, fixedLen)
			if err != nil {
				panic(fmt.Errorf("write-default binary/fixed (iceberg type %s): cannot decode string %q: %w", t, val, err))
			}
			s, err := scalar.MakeScalarParam(b, dt)
			if err != nil {
				panic(fmt.Errorf("write-default binary/fixed (iceberg type %s, value %v): %w", t, b, err))
			}

			return s
		}
		panic(fmt.Errorf("write-default binary/fixed: unsupported value type %T (%v)", v, v))
	// Float64, Bool, and String cast normally.
	// Int32 and Int64 arrive as float64 from JSON and are handled by MakeScalarParam.
	default:
		s, err := scalar.MakeScalarParam(v, dt)
		if err != nil {
			panic(fmt.Errorf("write-default (iceberg type %s, value %v %T): %w", t, v, v, err))
		}

		return s
	}
}

// defaultToArray creates an Arrow array of length n filled with the given default value v.
func defaultToArray(v any, t iceberg.Type, dt arrow.DataType, n int, alloc memory.Allocator) arrow.Array {
	sc := defaultToScalar(v, t, dt)
	out, err := scalar.MakeArrayFromScalar(sc, n, alloc)
	if err != nil {
		panic(fmt.Errorf("write-default (iceberg type %s, value %v %T): failed to create array: %w", t, v, v, err))
	}
	if _, ok := dt.(*extensions.UUIDType); ok {
		defer out.Release()

		data := array.NewData(dt, out.Len(), out.Data().Buffers(), nil, out.NullN(), 0)
		defer data.Release()

		return array.MakeFromData(data)
	}

	return out
}

type arrowProjectionVisitor struct {
	ctx                  context.Context
	fileSchema           *iceberg.Schema
	tableProperties      iceberg.Properties
	formatVersion        int
	includeFieldIDs      bool
	downcastNsTimestamp  bool
	useLargeTypes        bool
	useWriteDefault      bool
	allowMissingRequired bool
}

func (a *arrowProjectionVisitor) typeToArrowType(t iceberg.Type) arrow.DataType {
	return retOrPanic(TypeToArrowTypeWithOptions(t, ArrowSchemaOptions{
		IncludeFieldIDs: a.includeFieldIDs,
		UseLargeTypes:   a.useLargeTypes,
		TableProperties: a.tableProperties,
	}))
}

func (a *arrowProjectionVisitor) castIfNeeded(field iceberg.NestedField, vals arrow.Array) arrow.Array {
	fileField, ok := a.fileSchema.FindFieldByIDRef(field.ID, internal.SchemaRef{})
	if !ok {
		panic(fmt.Errorf("could not find field id %d in schema", field.ID))
	}

	typ, ok := fileField.Type.(iceberg.PrimitiveType)
	if !ok {
		vals.Retain()

		return vals
	}

	if !field.Type.Equals(typ) {
		if !canDowncastTimestampPrecision(fileField.Type, field.Type, a.downcastNsTimestamp) {
			var targetType arrow.DataType
			if a.formatVersion >= 3 && canPromoteDateToTimestamp(fileField.Type, field.Type) {
				targetType = a.typeToArrowType(field.Type)
			} else {
				promoted := retOrPanic(iceberg.PromoteType(fileField.Type, field.Type))
				targetType = a.typeToArrowType(promoted)
			}
			if !a.useLargeTypes {
				targetType = retOrPanic(ensureSmallArrowTypes(targetType))
			}

			return retOrPanic(compute.CastArray(a.ctx, vals,
				compute.SafeCastOptions(targetType)))
		}
	}

	targetType := a.typeToArrowType(field.Type)
	if !arrow.TypeEqual(targetType, vals.DataType()) {
		if out, ok := a.rewrapExtension(targetType, vals); ok {
			return out
		}

		switch field.Type.(type) {
		case iceberg.TimestampType:
			tt, tgtok := targetType.(*arrow.TimestampType)
			vt, valok := vals.DataType().(*arrow.TimestampType)

			if tgtok && valok && tt.TimeZone == "" && vt.TimeZone == "" && tt.Unit == arrow.Microsecond {
				if vt.Unit == arrow.Nanosecond && a.downcastNsTimestamp {
					return floorTimestampNanosecondsToMicroseconds(
						compute.GetAllocator(a.ctx), vals.(*array.Timestamp), tt)
				} else if vt.Unit == arrow.Second || vt.Unit == arrow.Millisecond {
					return retOrPanic(compute.CastArray(a.ctx, vals, compute.SafeCastOptions(tt)))
				}
			}

			panic(fmt.Errorf("unsupported schema projection from %s to %s",
				vals.DataType(), targetType))
		case iceberg.TimestampTzType:
			tt, tgtok := targetType.(*arrow.TimestampType)
			vt, valok := vals.DataType().(*arrow.TimestampType)

			if tgtok && valok && tt.TimeZone == "UTC" &&
				slices.Contains(utcAliases, vt.TimeZone) && tt.Unit == arrow.Microsecond {
				if vt.Unit == arrow.Nanosecond && a.downcastNsTimestamp {
					return floorTimestampNanosecondsToMicroseconds(
						compute.GetAllocator(a.ctx), vals.(*array.Timestamp), tt)
				} else if vt.Unit != arrow.Nanosecond {
					return retOrPanic(compute.CastArray(a.ctx, vals, compute.SafeCastOptions(tt)))
				}
			}

			panic(fmt.Errorf("unsupported schema projection from %s to %s",
				vals.DataType(), targetType))
		default:
			return retOrPanic(compute.CastArray(a.ctx, vals,
				compute.SafeCastOptions(targetType)))
		}
	}
	vals.Retain()

	return vals
}

// rewrapExtension re-wraps vals in targetType when both are extension types
// with the same extension name but differing extension parameters, returning
// false when the types are not such a pair. Arrow's compute layer has no
// extension-to-extension cast kernel, and the values are already in the
// extension's storage encoding, so only the storage array is cast and that only
// when the storage types differ. This is safe only because a shared extension
// name implies the storage payload is interpretation-compatible: parameters may
// differ, the encoded values do not.
func (a *arrowProjectionVisitor) rewrapExtension(targetType arrow.DataType, vals arrow.Array) (arrow.Array, bool) {
	tgtExt, ok := targetType.(arrow.ExtensionType)
	if !ok {
		return nil, false
	}

	srcExt, ok := vals.DataType().(arrow.ExtensionType)
	if !ok || srcExt.ExtensionName() != tgtExt.ExtensionName() {
		return nil, false
	}

	if arrow.TypeEqual(srcExt.StorageType(), tgtExt.StorageType()) {
		return array.NewExtensionArrayWithStorage(tgtExt,
			vals.(array.ExtensionArray).Storage()), true
	}

	storage := retOrPanic(compute.CastArray(a.ctx,
		vals.(array.ExtensionArray).Storage(),
		compute.SafeCastOptions(tgtExt.StorageType())))
	defer storage.Release()

	return array.NewExtensionArrayWithStorage(tgtExt, storage), true
}

func canDowncastTimestampPrecision(fileType, readType iceberg.Type, enabled bool) bool {
	if !enabled {
		return false
	}

	switch fileType.(type) {
	case iceberg.TimestampNsType:
		_, ok := readType.(iceberg.TimestampType)

		return ok
	case iceberg.TimestampTzNsType:
		_, ok := readType.(iceberg.TimestampTzType)

		return ok
	default:
		return false
	}
}

func canPromoteDateToTimestamp(fileType, readType iceberg.Type) bool {
	if _, ok := fileType.(iceberg.DateType); !ok {
		return false
	}

	switch readType.(type) {
	case iceberg.TimestampType, iceberg.TimestampNsType:
		return true
	default:
		return false
	}
}

func floorTimestampNanosecondsToMicroseconds(
	mem memory.Allocator,
	vals *array.Timestamp,
	targetType *arrow.TimestampType,
) arrow.Array {
	bldr := array.NewTimestampBuilder(mem, targetType)
	defer bldr.Release()

	for i := range vals.Len() {
		if vals.IsNull(i) {
			bldr.AppendNull()

			continue
		}

		bldr.Append(arrow.Timestamp(internal.FloorDiv(int64(vals.Value(i)), int64(1_000))))
	}

	return bldr.NewArray()
}

func (a *arrowProjectionVisitor) constructField(field iceberg.NestedField, arrowType arrow.DataType) arrow.Field {
	metadata := map[string]string{}
	if field.Doc != "" {
		metadata[ArrowFieldDocKey] = field.Doc
	}

	if a.includeFieldIDs {
		metadata[ArrowParquetFieldIDKey] = strconv.Itoa(field.ID)
	}

	return arrow.Field{
		Name:     field.Name,
		Type:     arrowType,
		Nullable: !field.Required,
		Metadata: arrow.MetadataFrom(metadata),
	}
}

func (a *arrowProjectionVisitor) Schema(_ *iceberg.Schema, _ arrow.Array, result arrow.Array) arrow.Array {
	return result
}

func ownsChildResult(arr arrow.Array) bool {
	if arr == nil {
		return false
	}

	_, nested := arr.DataType().(arrow.NestedType)

	return nested
}

func (a *arrowProjectionVisitor) Struct(st iceberg.StructType, structArr arrow.Array, fieldResults []arrow.Array) arrow.Array {
	if structArr == nil {
		return nil
	}

	fieldArrs := make([]arrow.Array, len(st.FieldList))
	fields := make([]arrow.Field, len(st.FieldList))
	for i, field := range st.FieldList {
		arr := fieldResults[i]
		if arr != nil {
			if ownsChildResult(arr) {
				defer arr.Release()
			}

			arr = a.castIfNeeded(field, arr)
			defer arr.Release()
			fieldArrs[i] = arr
			fields[i] = a.constructField(field, arr.DataType())
		} else {
			dt := a.typeToArrowType(field.Type)
			alloc := compute.GetAllocator(a.ctx)

			switch {
			case field.WriteDefault != nil && a.useWriteDefault:
				arr = defaultToArray(field.WriteDefault, field.Type, dt, structArr.Len(), alloc)
			case field.InitialDefault != nil && !a.useWriteDefault:
				arr = defaultToArray(field.InitialDefault, field.Type, dt, structArr.Len(), alloc)
			case !field.Required || a.allowMissingRequired:
				arr = array.MakeArrayOfNull(alloc, dt, structArr.Len())
			default:
				panic(fmt.Errorf("%w: required field is missing and has no default value: %s",
					iceberg.ErrInvalidSchema, field))
			}

			defer arr.Release()
			fieldArrs[i] = arr
			fields[i] = a.constructField(field, arr.DataType())
		}
	}

	var nullBitmap *memory.Buffer
	if structArr.NullN() > 0 {
		if structArr.Data().Offset() > 0 {
			// the children already accounted for any offset because we used the `Field` method
			// on the struct array in the FieldPartner accessor. So we just need to adjust the
			// bitmap to account for the offset.
			nullBitmap = memory.NewResizableBuffer(compute.GetAllocator(a.ctx))
			defer nullBitmap.Release()
			nullBitmap.Resize(int(bitutil.BytesForBits(int64(structArr.Len()))))

			bitutil.CopyBitmap(structArr.NullBitmapBytes(), structArr.Data().Offset(), structArr.Len(),
				nullBitmap.Bytes(), 0)

		} else {
			nullBitmap = structArr.Data().Buffers()[0]
		}
	}

	return retOrPanic(array.NewStructArrayWithFieldsAndNulls(fieldArrs, fields,
		nullBitmap, structArr.NullN(), 0))
}

func (a *arrowProjectionVisitor) Field(_ iceberg.NestedField, _ arrow.Array, fieldArr arrow.Array) arrow.Array {
	return fieldArr
}

func (a *arrowProjectionVisitor) List(listType iceberg.ListType, listArr arrow.Array, valArr arrow.Array) arrow.Array {
	if ownsChildResult(valArr) {
		defer valArr.Release()
	}

	arr, ok := listArr.(array.ListLike)
	if !ok || valArr == nil {
		return nil
	}

	valArr = a.castIfNeeded(listType.ElementField(), valArr)
	defer valArr.Release()

	var outType arrow.ListLikeType
	elemField := a.constructField(listType.ElementField(), valArr.DataType())
	switch arr.DataType().ID() {
	case arrow.LIST:
		outType = arrow.ListOfField(elemField)
	case arrow.LARGE_LIST:
		outType = arrow.LargeListOfField(elemField)
	case arrow.LIST_VIEW:
		outType = arrow.LargeListViewOfField(elemField)
	}

	data := array.NewData(outType, arr.Len(), arr.Data().Buffers(),
		[]arrow.ArrayData{valArr.Data()}, arr.NullN(), arr.Data().Offset())
	defer data.Release()

	out := array.MakeFromData(data)

	// The Parquet reader may return large_list offsets even when the schema
	// returned alongside the records (built with useLargeTypes) asks for small
	// list, or vice versa; coerce the offset width to match so the writer does
	// not reject the batch. A plain relabel would misread int64 offsets as int32.
	switch out.DataType().ID() {
	case arrow.LIST, arrow.LARGE_LIST:
		var want arrow.DataType = arrow.ListOfField(elemField)
		if a.useLargeTypes {
			want = arrow.LargeListOfField(elemField)
		}
		if !arrow.TypeEqual(out.DataType(), want) {
			defer out.Release()

			return retOrPanic(compute.CastArray(a.ctx, out, compute.SafeCastOptions(want)))
		}
	}

	return out
}

func (a *arrowProjectionVisitor) Map(m iceberg.MapType, mapArray, keyResult, valResult arrow.Array) arrow.Array {
	if ownsChildResult(keyResult) {
		defer keyResult.Release()
	}

	if ownsChildResult(valResult) {
		defer valResult.Release()
	}

	if keyResult == nil || valResult == nil {
		return nil
	}

	arr, ok := mapArray.(*array.Map)
	if !ok {
		return nil
	}

	keys := a.castIfNeeded(m.KeyField(), keyResult)
	defer keys.Release()
	vals := a.castIfNeeded(m.ValueField(), valResult)
	defer vals.Release()

	keyField := a.constructField(m.KeyField(), keys.DataType())
	valField := a.constructField(m.ValueField(), vals.DataType())

	mapType := arrow.MapOfWithMetadata(keyField.Type, keyField.Metadata, valField.Type, valField.Metadata)
	childData := array.NewData(mapType.Elem(), arr.Data().Children()[0].Len(), []*memory.Buffer{nil},
		[]arrow.ArrayData{keys.Data(), vals.Data()}, 0, 0)
	defer childData.Release()
	newData := array.NewData(mapType, arr.Len(), arr.Data().Buffers(),
		[]arrow.ArrayData{childData}, arr.NullN(), arr.Offset())
	defer newData.Release()

	return array.NewMapData(newData)
}

func (a *arrowProjectionVisitor) Primitive(_ iceberg.PrimitiveType, arr arrow.Array) arrow.Array {
	return arr
}

// Variant reassembles a shredded variant back into the unshredded layout so the
// projected schema matches; without it the scanner fails to assemble the table.
func (a *arrowProjectionVisitor) Variant(_ iceberg.VariantType, arr arrow.Array) arrow.Array {
	if arr == nil {
		return arr
	}
	varr, ok := arr.(*extensions.VariantArray)
	if !ok || !varr.IsShredded() {
		return arr
	}
	// UnshredVariant returns a caller-owned array whose ref is balanced by
	// castIfNeeded's Retain/Release. This holds only because VariantType is not
	// an arrow.NestedType; if it were, ownsChildResult would report true and the
	// parent Struct/List/Map would Release and double-free.
	// The checked-allocator leak tests guard this invariant.
	out, err := extensions.UnshredVariant(varr, compute.GetAllocator(a.ctx))
	if err != nil {
		panic(fmt.Errorf("variant projection: %w", err))
	}

	return out
}

// SchemaOptions controls the behaviour of ToRequestedSchema.
type SchemaOptions struct {
	DowncastTimestamp bool
	// FormatVersion enables format-version-specific read promotions.
	FormatVersion        int
	IncludeFieldIDs      bool
	UseLargeTypes        bool
	UseWriteDefault      bool
	AllowMissingRequired bool
	TableProperties      iceberg.Properties
}

// ToRequestedSchema will construct a new record batch matching the requested iceberg schema
// casting columns if necessary as appropriate.
func ToRequestedSchema(ctx context.Context, requested, fileSchema *iceberg.Schema, batch arrow.RecordBatch, opts SchemaOptions) (arrow.RecordBatch, error) {
	return toRequestedSchema(ctx, requested, fileSchema, batch, opts, nil)
}

// toRequestedSchema is the internal write-path variant of ToRequestedSchema.
// When targetArrowSchema is provided, an exact Arrow schema match can reuse the
// input batch without walking the Iceberg schema or rebuilding any arrays.
func toRequestedSchema(ctx context.Context, requested, fileSchema *iceberg.Schema, batch arrow.RecordBatch, opts SchemaOptions, targetArrowSchema *arrow.Schema) (arrow.RecordBatch, error) {
	if targetArrowSchema != nil && arrowSchemaEqual(batch.Schema(), targetArrowSchema) {
		batch.Retain()

		return batch, nil
	}

	st := array.RecordToStructArray(batch)
	defer st.Release()

	result, err := iceberg.VisitSchemaWithPartner[arrow.Array, arrow.Array](requested, st,
		&arrowProjectionVisitor{
			ctx:                  ctx,
			fileSchema:           fileSchema,
			tableProperties:      opts.TableProperties,
			formatVersion:        opts.FormatVersion,
			includeFieldIDs:      opts.IncludeFieldIDs,
			downcastNsTimestamp:  opts.DowncastTimestamp,
			useLargeTypes:        opts.UseLargeTypes,
			useWriteDefault:      opts.UseWriteDefault,
			allowMissingRequired: opts.AllowMissingRequired,
		}, arrowAccessor{fileSchema: fileSchema})
	if err != nil {
		return nil, err
	}
	out := array.RecordFromStructArray(result.(*array.Struct), nil)
	result.Release()

	return out, nil
}

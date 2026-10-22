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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/geoarrow/geoarrow-go"
)

func checkCRSString(rawCrs json.RawMessage) bool {
	b := bytes.TrimSpace(rawCrs)

	return len(b) > 0 && b[0] == '"'
}

func checkCRSJSON(rawCrs json.RawMessage) bool {
	b := bytes.TrimSpace(rawCrs)

	return len(b) > 0 && b[0] == '{'
}

// errWKT2CRSNotSupported is returned for WKT2:2019 CRS definitions, which this
// package does not support yet.
var errWKT2CRSNotSupported = errors.New("CRS type wkt2:2019 not supported")

// errPROJJSONStringCRSNotSupported is returned for projjson-typed string CRS
// values, which this package does not support yet.
var errPROJJSONStringCRSNotSupported = errors.New("CRS type projjson not supported for string CRS")

// isWKT2CRSString reports whether crs looks like a WKT2 (ISO 19162) CRS
// definition. It is a best-effort heuristic used only to surface the clearer
// errWKT2CRSNotSupported message. The list covers the WKT2:2019 CRS keywords
// and their long forms.
func isWKT2CRSString(crs string) bool {
	upper := strings.ToUpper(strings.TrimSpace(crs))

	for _, prefix := range []string{
		"BOUNDCRS[",
		"COMPOUNDCRS[",
		"COORDINATEMETADATA[",
		"COORDINATEOPERATION[",
		"CRS[",
		"DERIVEDPROJCRS[",
		"ENGCRS[",
		"ENGINEERINGCRS[",
		"GEODCRS[",
		"GEODETICCRS[",
		"GEOGCRS[",
		"GEOGRAPHICCRS[",
		"PARAMETRICCRS[",
		"PROJCRS[",
		"PROJECTEDCRS[",
		"TIMECRS[",
		"VERTCRS[",
		"VERTICALCRS[",
	} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}

	return false
}

// geoArrowCRSToIcebergCRS maps GeoArrow CRS metadata to an Iceberg CRS string.
// Absent CRS metadata means the default CRS OGC:CRS84, matching the Parquet
// geospatial spec and Iceberg's default geometry/geography types.
func geoArrowCRSToIcebergCRS(meta geoarrow.Metadata) (string, error) {
	if len(meta.CRS) == 0 {
		return iceberg.DefaultGeoCRS, nil
	}

	switch {
	case checkCRSString(meta.CRS):
		var crs string

		if err := json.Unmarshal(meta.CRS, &crs); err != nil {
			return "", fmt.Errorf("invalid geoarrow CRS metadata: %w", err)
		}
		if crs == "" {
			return "", errors.New("unsupported CRS: empty string CRS")
		}

		if strings.EqualFold(crs, iceberg.DefaultGeoCRS) || strings.EqualFold(crs, "EPSG:4326") {
			return iceberg.DefaultGeoCRS, nil
		}

		switch meta.CRSType {
		case geoarrow.CRSTypePROJJSON:
			return "", errPROJJSONStringCRSNotSupported
		case geoarrow.CRSTypeWKT22019:
			return "", errWKT2CRSNotSupported
		default:
			// Unset, AuthorityCode, SRID, and future CRSType values continue to
			// the string-shape checks below.
		}

		if isWKT2CRSString(crs) {
			return "", errWKT2CRSNotSupported
		}
		if meta.CRSType == geoarrow.CRSTypeSRID {
			// Some writers store the identifier already prefixed; don't prefix twice.
			if strings.HasPrefix(strings.ToLower(crs), "srid:") {
				return crs, nil
			}

			return "srid:" + crs, nil
		}

		// Bare or unrecognized string CRS values are opaque identifiers per the
		// Iceberg type contract; preserve them for read/write compatibility.
		return crs, nil
	case checkCRSJSON(meta.CRS):
		if meta.CRSType == geoarrow.CRSTypePROJJSON {
			return "", errors.New("unsupported CRS: projjson metadata requires table schema and properties to recover the Iceberg CRS reference")
		}

		var crs map[string]json.RawMessage

		if err := json.Unmarshal(meta.CRS, &crs); err != nil {
			return "", fmt.Errorf("invalid geoarrow CRS metadata: %w", err)
		}

		idRaw, ok := crs["id"]
		if !ok || len(idRaw) == 0 {
			return "", errors.New("unsupported CRS: missing id")
		}

		var id map[string]json.RawMessage
		if err := json.Unmarshal(idRaw, &id); err != nil {
			return "", fmt.Errorf("unsupported CRS: invalid id: %w", err)
		}

		var authority string
		authorityRaw, ok := id["authority"]
		if !ok || len(authorityRaw) == 0 {
			return "", errors.New("unsupported CRS: missing id.authority")
		}
		if err := json.Unmarshal(authorityRaw, &authority); err != nil {
			return "", fmt.Errorf("unsupported CRS: invalid id.authority: %w", err)
		}
		if authority == "" {
			return "", errors.New("unsupported CRS: empty id.authority")
		}

		codeRaw := id["code"]
		if len(codeRaw) == 0 {
			return "", errors.New("unsupported CRS: missing id.code")
		}

		var code string
		if err := json.Unmarshal(codeRaw, &code); err != nil {
			var codeNum json.Number
			if err := json.Unmarshal(codeRaw, &codeNum); err != nil {
				return "", fmt.Errorf("unsupported CRS: invalid id.code: %w", err)
			}
			code = codeNum.String()
		}
		if code == "" {
			return "", errors.New("unsupported CRS: empty id.code")
		}

		authorityCode := authority + ":" + code
		if strings.EqualFold(authorityCode, iceberg.DefaultGeoCRS) || strings.EqualFold(authorityCode, "EPSG:4326") {
			return iceberg.DefaultGeoCRS, nil
		}

		return authorityCode, nil
	default:
		return "", errors.New("unsupported CRS: CRS must either be omitted, a string, or a JSON object")
	}
}

func (c convertToIceberg) geoArrowMetadataToIcebergType(meta geoarrow.Metadata) (iceberg.Type, error) {
	crs, err := c.geoArrowCRSToIcebergCRS(meta)
	if err != nil {
		return nil, err
	}

	return geoArrowMetadataToIcebergTypeWithCRS(meta, crs)
}

func (c convertToIceberg) geoArrowCRSToIcebergCRS(meta geoarrow.Metadata) (string, error) {
	if meta.CRSType != geoarrow.CRSTypePROJJSON || !checkCRSJSON(meta.CRS) {
		return geoArrowCRSToIcebergCRS(meta)
	}

	fieldID, ok := c.currentFieldID()
	if !ok {
		return "", errors.New("unsupported CRS: projjson metadata requires field IDs to recover the Iceberg CRS reference")
	}

	return projJSONCRSFromTable(fieldID, meta.CRS, c.tableSchema, c.tableProperties)
}

func geoArrowMetadataToIcebergType(meta geoarrow.Metadata) (iceberg.Type, error) {
	crs, err := geoArrowCRSToIcebergCRS(meta)
	if err != nil {
		return nil, err
	}

	return geoArrowMetadataToIcebergTypeWithCRS(meta, crs)
}

func geoArrowMetadataToIcebergTypeWithCRS(meta geoarrow.Metadata, crs string) (iceberg.Type, error) {
	// Missing GeoArrow edges use the planar default, matching arrow-rs
	// geoarrow.wkb reads. PyIceberg-authored geography files may omit edges,
	// which is an interop gap: Arrow metadata alone cannot distinguish those
	// geography columns from Geometry here.
	switch meta.Edges {
	case geoarrow.EdgePlanar:
		return iceberg.GeometryTypeOf(crs)
	case geoarrow.EdgeVincenty, geoarrow.EdgeKarney, geoarrow.EdgeThomas, geoarrow.EdgeAndoyer, geoarrow.EdgeSpherical:
		return iceberg.GeographyTypeOf(crs, string(meta.Edges))
	default:
		return nil, fmt.Errorf("unsupported geoarrow edges %q", meta.Edges)
	}
}

var authorityCodeCRS = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z0-9_.-]+$`)

// icebergCRSToGeoArrowMetadata converts an Iceberg CRS string to GeoArrow
// metadata. props is used only for projjson:<property> references, where the
// property value supplies the PROJJSON document embedded into GeoArrow metadata.
func icebergCRSToGeoArrowMetadata(crs string, props iceberg.Properties) (geoarrow.Metadata, error) {
	lowerCRS := strings.ToLower(crs)
	if strings.HasPrefix(lowerCRS, "srid:") {
		id := crs[len("srid:"):]

		raw, _ := json.Marshal(id) //nolint:errcheck // Marshalling a string can't fail

		return geoarrow.Metadata{
			CRS:     raw,
			CRSType: geoarrow.CRSTypeSRID,
		}, nil
	}

	if strings.HasPrefix(lowerCRS, "projjson:") {
		// The scheme is case-insensitive, but the table property key is not.
		propKey := strings.TrimSpace(crs[len("projjson:"):])
		if propKey == "" {
			return geoarrow.Metadata{}, fmt.Errorf("%w: projjson CRS missing table property key", iceberg.ErrInvalidSchema)
		}
		if props == nil {
			return geoarrow.Metadata{}, fmt.Errorf("%w: projjson CRS %q could not be resolved from table properties", iceberg.ErrInvalidSchema, crs)
		}

		projjson, ok := props[propKey]
		if !ok || strings.TrimSpace(projjson) == "" {
			return geoarrow.Metadata{}, fmt.Errorf("%w: projjson CRS %q references missing table property %q", iceberg.ErrInvalidSchema, crs, propKey)
		}

		raw := json.RawMessage(projjson)
		if !json.Valid(raw) {
			return geoarrow.Metadata{}, fmt.Errorf("%w: projjson CRS table property %q is not valid JSON", iceberg.ErrInvalidSchema, propKey)
		}

		return geoarrow.Metadata{
			CRS:     raw,
			CRSType: geoarrow.CRSTypePROJJSON,
		}, nil
	}

	var raw []byte

	if lowerCRS == "epsg:4326" {
		// collapse EPSG:4326 to OGC:CRS84
		raw, _ = json.Marshal(iceberg.DefaultGeoCRS) //nolint:errcheck // Marshalling a string can't fail
	} else {
		raw, _ = json.Marshal(crs) //nolint:errcheck // Marshalling a string can't fail
	}

	meta := geoarrow.Metadata{CRS: raw}
	if authorityCodeCRS.MatchString(crs) {
		meta.CRSType = geoarrow.CRSTypeAuthorityCode
	}

	return meta, nil
}

func projJSONCRSFromTable(fieldID int, rawCRS json.RawMessage, tableSchema *iceberg.Schema, props iceberg.Properties) (string, error) {
	if tableSchema == nil || props == nil {
		return "", errors.New("unsupported CRS: projjson metadata requires table schema and properties to recover the Iceberg CRS reference")
	}

	field, ok := tableSchema.FindFieldByID(fieldID)
	if !ok {
		return "", fmt.Errorf("unsupported CRS: projjson field id %d not found in table schema", fieldID)
	}

	var crs string
	switch typ := field.Type.(type) {
	case iceberg.GeometryType:
		crs = typ.CRS()
	case iceberg.GeographyType:
		crs = typ.CRS()
	default:
		return "", fmt.Errorf("unsupported CRS: table field id %d is %s, not a geo type", fieldID, field.Type)
	}

	if !strings.HasPrefix(strings.ToLower(crs), "projjson:") {
		return "", fmt.Errorf("unsupported CRS: table field id %d has CRS %q, not a projjson reference", fieldID, crs)
	}

	propKey := strings.TrimSpace(crs[len("projjson:"):])
	if propKey == "" {
		return "", fmt.Errorf("%w: projjson CRS missing table property key", iceberg.ErrInvalidSchema)
	}

	projjson, ok := props[propKey]
	if !ok || strings.TrimSpace(projjson) == "" {
		return "", fmt.Errorf("%w: projjson CRS %q references missing table property %q", iceberg.ErrInvalidSchema, crs, propKey)
	}

	if !json.Valid([]byte(projjson)) {
		return "", fmt.Errorf("%w: projjson CRS table property %q is not valid JSON", iceberg.ErrInvalidSchema, propKey)
	}

	if !jsonEqual(rawCRS, []byte(projjson)) {
		return "", fmt.Errorf("unsupported CRS: projjson metadata for field id %d does not match table property %q", fieldID, propKey)
	}

	return crs, nil
}

func jsonEqual(lhs, rhs []byte) bool {
	var lhsCompact, rhsCompact bytes.Buffer
	if err := json.Compact(&lhsCompact, lhs); err != nil {
		return false
	}
	if err := json.Compact(&rhsCompact, rhs); err != nil {
		return false
	}

	return bytes.Equal(lhsCompact.Bytes(), rhsCompact.Bytes())
}

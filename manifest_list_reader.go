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

package iceberg

import (
	"fmt"
	"io"
	"strconv"

	"github.com/twmb/avro"
	"github.com/twmb/avro/ocf"
)

// Manifest-list entry field IDs used to infer the format version from an
// embedded writer schema. Per the Iceberg spec, content and sequence_number
// are required fields for v2+ manifest lists and first_row_id is a v3 field;
// none of them exist in v1.
const (
	fieldIDManifestSequenceNumber = 515
	fieldIDManifestContent        = 517
	fieldIDManifestFirstRowID     = 520
)

// schemaFieldID returns the Iceberg "field-id" property of an Avro schema
// field, if present. Schemas parsed from JSON (e.g. an OCF header's embedded
// writer schema) carry numbers as float64, while schemas built in memory
// carry int.
func schemaFieldID(f avro.SchemaField) (int, bool) {
	switch v := f.Props["field-id"].(type) {
	case int:
		return v, true
	case int64:
		return int(v), true
	case float64:
		return int(v), true
	}

	return 0, false
}

// inferManifestListVersion infers the format version of a manifest list from
// the fields of its embedded writer schema: first_row_id (field-id 520)
// implies v3, content (517) or sequence_number (515) imply v2, and a schema
// with none of them is v1. Fields are identified by their field-id property —
// the stable contract per the spec — with the spec field names as a fallback
// for writers that don't annotate IDs.
func inferManifestListVersion(sc *avro.Schema) int {
	version := 1
	for _, f := range sc.Root().Fields {
		id, ok := schemaFieldID(f)
		switch {
		case ok && id == fieldIDManifestFirstRowID,
			!ok && f.Name == "first_row_id":
			return 3
		case ok && (id == fieldIDManifestContent || id == fieldIDManifestSequenceNumber),
			!ok && (f.Name == "content" || f.Name == "sequence_number"):
			version = 2
		}
	}

	return version
}

// ReadManifestList reads in an avro manifest list file and returns a slice
// of manifest files or an error if one is encountered.
//
// Per the Iceberg spec, manifest list files are not required to carry a
// "format-version" metadata key (only manifest files are). When the key is
// absent, the version is inferred from the embedded writer schema, so lists
// from writers that omit the key (e.g. DuckDB's iceberg extension) decode
// with their real content types and sequence numbers instead of falling back
// to v1. When the key is present but claims a lower version than the schema
// carries fields for, an error is returned rather than silently dropping
// those fields on decode (or on a later rewrite through the older writer
// schema).
func ReadManifestList(in io.Reader) ([]ManifestFile, error) {
	var version int

	rd, err := ocf.NewReader(in, ocf.WithReaderSchemaFunc(func(rd *ocf.Reader) (*avro.Schema, error) {
		inferred := inferManifestListVersion(rd.Schema())

		version = inferred
		if raw := rd.Metadata()["format-version"]; len(raw) > 0 {
			v, err := strconv.Atoi(string(raw))
			if err != nil {
				return nil, fmt.Errorf("invalid format-version: %w", err)
			}

			version = v
		}
		if err := validateManifestFormatVersion(version); err != nil {
			return nil, err
		}

		if version < inferred {
			return nil, fmt.Errorf("manifest list's 'format-version' metadata says %d, but the embedded writer schema carries v%d fields; reading it as v%d would silently drop them", version, inferred, version)
		}

		if version == 1 {
			return manifestFileV1Reader, nil
		}

		return manifestFileReader, nil
	}))
	if err != nil {
		return nil, err
	}

	if version == 1 {
		return decodeV1Manifests(rd)
	}

	return decodeManifests[*manifestFile](rd, version)
}

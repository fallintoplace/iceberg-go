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
	"testing"

	"github.com/apache/iceberg-go"
)

func TestNameMappingString(t *testing.T) {
	parentID, childID := 1, 2
	field := iceberg.MappedField{
		FieldID: &parentID,
		Names:   []string{"payload", "legacy"},
		Fields: []iceberg.MappedField{
			{FieldID: &childID, Names: []string{"child"}},
			{Names: []string{"optional", "以前"}},
		},
	}

	const wantField = "([payload, legacy] -> 1 ([child] -> 2), ([optional, 以前] -> ?))"
	if got := field.String(); got != wantField {
		t.Fatalf("MappedField.String() = %q, want %q", got, wantField)
	}

	mapping := iceberg.NameMapping{field, {Names: []string{"empty"}}}
	const wantMapping = "[\n\t([payload, legacy] -> 1 ([child] -> 2), ([optional, 以前] -> ?))\n\t([empty] -> ?)\n]"
	if got := mapping.String(); got != wantMapping {
		t.Fatalf("NameMapping.String() = %q, want %q", got, wantMapping)
	}

	var empty iceberg.NameMapping
	if got := empty.String(); got != "[\n]" {
		t.Fatalf("empty NameMapping.String() = %q, want %q", got, "[\n]")
	}

	unnamed := iceberg.MappedField{}
	if got := unnamed.String(); got != "([] -> ?)" {
		t.Fatalf("unnamed MappedField.String() = %q, want %q", got, "([] -> ?)")
	}
}

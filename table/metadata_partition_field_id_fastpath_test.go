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
	"encoding/json"
	"testing"
)

func TestPartitionFieldIDNormalizationFastPath(t *testing.T) {
	cases := []struct {
		name       string
		specs      string
		needsWrite bool
		wantErr    bool
	}{
		{"canonical", `"partition-specs":[{"spec-id":0,"fields":[{"field-id":1000,"name":"p"}]}]`, false, false},
		{"whitespace before colon", `"partition-specs":[{"spec-id":0,"fields":[{"field-id"  :1000}]}]`, false, false},
		{"null present ID", `"partition-specs":[{"spec-id":0,"fields":[{"field-id":null}]}]`, false, false},
		{"multiple specs", `"partition-specs":[{"spec-id":0,"fields":[{"field-id":1000}]},{"spec-id":1,"fields":[{"field-id":1001}]}]`, false, false},
		{"missing field ID", `"partition-specs":[{"spec-id":0,"fields":[{"name":"p"}]}]`, true, false},
		{"missing among other IDs", `"partition-specs":[{"spec-id":0,"fields":[{"field-id":1000},{"name":"p"}]}]`, true, false},
		{"case-folded key must be normalized", `"partition-specs":[{"spec-id":0,"fields":[{"FIELD-ID":42}]}]`, true, false},
		{"escaped key preserves existing behavior", `"partition-specs":[{"spec-id":0,"fields":[{"\u0066ield-id":1000}]}]`, false, false},
		{"nested key does not count", `"partition-specs":[{"spec-id":0,"fields":[{"details":{"field-id":9}}]}]`, true, false},
		{"string value does not count", `"partition-specs":[{"spec-id":0,"fields":[{"name":"field-id"}]}]`, true, false},
		{"legacy canonical", `"partition-spec":[{"field-id":1000}]`, false, false},
		{"legacy missing", `"partition-spec":[{"name":"p"}]`, true, false},
		{"missing spec ID", `"partition-specs":[{"fields":[{"field-id":1000}]}]`, false, true},
		{"non-object field", `"partition-specs":[{"spec-id":0,"fields":[123]}]`, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(`{"last-updated-ms":0,"last-partition-id":1000,` + tc.specs + `}`)
			normalized, err := assignMissingPartitionFieldIDs(data)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			same := string(normalized) == string(data)
			if tc.needsWrite && same {
				t.Fatal("expected normalization")
			}
			if !tc.needsWrite && !same {
				t.Fatalf("unchanged metadata was rewritten: %s", normalized)
			}
			if !json.Valid(normalized) {
				t.Fatal("invalid normalized JSON")
			}
		})
	}
}

func TestPartitionFieldIDFastPathKeepsMixedLegacySpecPriority(t *testing.T) {
	// The spec list has priority over the legacy single-spec field list.
	data := []byte(`{"last-updated-ms":0,"partition-specs":[],"partition-spec":[{}]}`)
	got, err := assignMissingPartitionFieldIDs(data)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("unexpected rewrite: %s", got)
	}
}

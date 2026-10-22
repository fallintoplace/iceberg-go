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
	"fmt"
	"testing"
)

var partitionSpecIDBenchSink bool

// The baseline reproduces the previous per-field map parsing and exact-key check.
func benchmarkPartitionSpecIDMapProbe(raw json.RawMessage) (bool, error) {
	var specs []rawPartitionSpec
	if err := json.Unmarshal(raw, &specs); err != nil {
		return false, err
	}
	if err := validatePartitionSpecIDs(specs); err != nil {
		return false, err
	}
	for _, spec := range specs {
		for _, field := range spec.Fields {
			if _, ok := field["field-id"]; !ok {
				return true, nil
			}
		}
	}

	return false, nil
}

func benchmarkPartitionSpecIDData(specCount, fieldCount int) json.RawMessage {
	type field struct {
		Name      string `json:"name"`
		Transform string `json:"transform"`
		SourceID  int    `json:"source-id"`
		FieldID   int    `json:"field-id"`
	}
	type spec struct {
		ID     int     `json:"spec-id"`
		Fields []field `json:"fields"`
	}
	specs := make([]spec, specCount)
	for i := range specs {
		specs[i].ID = i
		specs[i].Fields = make([]field, fieldCount)
		for j := range specs[i].Fields {
			specs[i].Fields[j] = field{
						Name:      fmt.Sprintf("p_%d", j),
				Transform: "identity",
				SourceID:  j + 1,
				FieldID:   1000 + i*fieldCount + j,
			}
		}
	}
	out, err := json.Marshal(specs)
	if err != nil {
		panic(err)
	}

	return out
}

// Compare the old map-based check with the exact-key raw-field probe in the
// same process, excluding metadata unmarshalling and any snapshot validation.
func BenchmarkPartitionSpecFieldIDProbe(b *testing.B) {
	for _, tc := range []struct {
		specs  int
		fields int
	}{
		{64, 1}, {64, 8}, {256, 1}, {256, 8}, {1024, 8},
	} {
		raw := benchmarkPartitionSpecIDData(tc.specs, tc.fields)
		for _, method := range []struct {
			name string
			probe func(json.RawMessage) (bool, error)
		}{
			{"Before", benchmarkPartitionSpecIDMapProbe},
			{"After", partitionSpecsNeedFieldIDNormalization},
		} {
			b.Run(fmt.Sprintf("%s/specs=%d/fields=%d", method.name, tc.specs, tc.fields), func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(len(raw)))
				b.ResetTimer()
				for range b.N {
					needed, err := method.probe(raw)
					if err != nil {
						b.Fatal(err)
					}
					partitionSpecIDBenchSink = needed
				}
			})
		}
	}
}

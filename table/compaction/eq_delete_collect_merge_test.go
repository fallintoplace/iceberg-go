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

package compaction

import (
	"reflect"
	"testing"
)

func TestSurvivorSurveyMergeWithSpecs(t *testing.T) {
	type record struct {
		specID    int32
		partition map[int]any
		sequence  int64
	}
	groups := [][]record{
		{{1, map[int]any{100: int32(1)}, 7}, {2, nil, 11}},
		{{1, map[int]any{100: int32(1)}, 3}, {2, map[int]any{100: int32(2)}, -1}},
		{{1, nil, 5}, {2, map[int]any{100: int32(2)}, 12}},
		{{1, map[int]any{100: int32(3)}, 20}},
	}
	sequential := NewSurvivorSurvey()
	merged := NewSurvivorSurvey()
	for _, group := range groups {
		partial := NewSurvivorSurvey()
		for _, rec := range group {
			sequential.AddSurvivorWithSpec(rec.specID, rec.partition, rec.sequence)
			partial.AddSurvivorWithSpec(rec.specID, rec.partition, rec.sequence)
		}
		merged.merge(partial)
	}
	if !reflect.DeepEqual(merged, sequential) {
		t.Fatalf("merged spec-aware survey differs from sequential: got %+v, want %+v", merged, sequential)
	}
}

func TestSurvivorSurveyMergeConservative(t *testing.T) {
	groups := [][]struct {
		partition map[int]any
		sequence  int64
	}{
		{{map[int]any{10: int32(1)}, 5}, {nil, 8}},
		{{map[int]any{10: int32(1)}, 3}},
		{{nil, -1}, {map[int]any{10: int32(2)}, 9}},
	}
	sequential, merged := NewSurvivorSurvey(), NewSurvivorSurvey()
	for _, group := range groups {
		partial := NewSurvivorSurvey()
		for _, rec := range group {
			sequential.AddSurvivor(rec.partition, rec.sequence)
			partial.AddSurvivor(rec.partition, rec.sequence)
		}
		merged.merge(partial)
	}
	if !reflect.DeepEqual(merged, sequential) {
		t.Fatalf("merged conservative survey differs from sequential: got %+v, want %+v", merged, sequential)
	}
}

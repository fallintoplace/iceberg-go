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

package substrait

import (
	"reflect"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestToSubstraitLiteralSetSorting(t *testing.T) {
	tests := []struct {
		name  string
		typ   iceberg.Type
		input []iceberg.Literal
		want  []iceberg.Literal
	}{
		{
			name:  "string",
			typ:   iceberg.PrimitiveTypes.String,
			input: []iceberg.Literal{iceberg.StringLiteral("zebra"), iceberg.StringLiteral("apple"), iceberg.StringLiteral("banana")},
			want:  []iceberg.Literal{iceberg.StringLiteral("apple"), iceberg.StringLiteral("banana"), iceberg.StringLiteral("zebra")},
		},
		{
			name:  "bool",
			typ:   iceberg.PrimitiveTypes.Bool,
			input: []iceberg.Literal{iceberg.BoolLiteral(true), iceberg.BoolLiteral(false)},
			want:  []iceberg.Literal{iceberg.BoolLiteral(false), iceberg.BoolLiteral(true)},
		},
		{
			name:  "int64",
			typ:   iceberg.PrimitiveTypes.Int64,
			input: []iceberg.Literal{iceberg.Int64Literal(2), iceberg.Int64Literal(1), iceberg.Int64Literal(10)},
			want:  []iceberg.Literal{iceberg.Int64Literal(1), iceberg.Int64Literal(10), iceberg.Int64Literal(2)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lits := append([]iceberg.Literal(nil), tt.input...)
			if result := toSubstraitLiteralSet(tt.typ, lits); result == nil {
				t.Fatal("unexpected nil literal set")
			}
			if !reflect.DeepEqual(lits, tt.want) {
				t.Fatalf("sorted literal set = %v, want %v", lits, tt.want)
			}
		})
	}
}

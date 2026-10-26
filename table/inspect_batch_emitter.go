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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// inspectBatchEmitter manages batch boundaries for streaming metadata tables.
// Callers append rows to builder before calling appendRows and own its Release.
// An empty result still emits one empty batch.
type inspectBatchEmitter struct {
	builder *array.RecordBuilder
	yield   func(arrow.RecordBatch, error) bool
	rows    int
	emitted bool
}

// appendRows records rows already appended to builder and stops when the
// consumer declines a full batch.
func (e *inspectBatchEmitter) appendRows(n int) bool {
	e.rows += n
	if e.rows == inspectRecordBatchSize {
		return e.emit()
	}

	return true
}

func (e *inspectBatchEmitter) emit() bool {
	if e.rows == 0 {
		return true
	}
	batch := e.builder.NewRecordBatch()
	e.rows = 0
	e.emitted = true

	return e.yield(batch, nil)
}

func (e *inspectBatchEmitter) finish() {
	if e.rows > 0 {
		_ = e.emit()
	} else if !e.emitted {
		_ = e.yield(e.builder.NewRecordBatch(), nil)
	}
}

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

// partitionRecordIndex maps a partition record to its first-seen group index.
// The existing partitionMapNode supplies the same typed keys and float/byte
// equality as the fanout writer, without scanning all earlier groups.
//
// Only index values are retained: the caller retains first-seen records in its
// output slice, while binary keys are copied into the map on insertion.
type partitionRecordIndex struct {
	root       partitionMapNode
	hasEmpty   bool
	emptyIndex int
}

// getOrInsert returns the existing index, or installs next for a new key.
// Callers pass their current group count as next.
func (idx *partitionRecordIndex) getOrInsert(key partitionRecord, next int) int {
	if len(key) == 0 {
		if !idx.hasEmpty {
			idx.hasEmpty = true
			idx.emptyIndex = next
		}

		return idx.emptyIndex
	}

	node := &idx.root
	for _, part := range key[:len(key)-1] {
		child, ok := node.child(part)
		if !ok {
			nextNode := newPartitionMapNode()
			node.setChild(part, nextNode)
			node = nextNode
		} else {
			node = child.(*partitionMapNode)
		}
	}

	last := key[len(key)-1]
	if found, ok := node.child(last); ok {
		return found.(int)
	}
	node.setChild(last, next)

	return next
}

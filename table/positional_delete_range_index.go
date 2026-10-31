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
	"cmp"
	"slices"

	"github.com/apache/iceberg-go"
)

// A path range index is useful when a large partition has many positional
// delete files whose file_path bounds overlap only a small fraction of data
// paths. Every node caches the maximum upper bound and sequence number in its
// subtree. Queries skip entire subtrees that cannot contain matching deletes.
// Node entry indexes refer to the original sequence-sorted partition bucket.
// Reordering only the selected indexes preserves stable delete-file ordering.
type positionalDeletePathRangeIndex struct {
	nodes []positionalDeletePathRangeNode
	root  int
}

type positionalDeletePathRangeNode struct {
	entryIndex     int
	left, right    int
	minLower       string
	maxUpper       string
	unboundedUpper bool
	maxSequence    int64
}

const (
	positionalPathRangeMinEntries = 128
	// A large fraction of unbounded ranges prevents the index from pruning.
	positionalPathRangeMinBoundedPercent = 75
)

func buildPositionalDeletePathRangeIndex(entries []positionalPartitionDeleteEntry) *positionalDeletePathRangeIndex {
	if len(entries) < positionalPathRangeMinEntries {
		return nil
	}

	// Avoid building temporary arrays for obviously broad, heavily
	// overlapping bounds. This sample is only an optimization decision: an
	// underestimated overlap still goes through the exact sweep below.
	probe := entries[len(entries)/2].pathMetrics.lower
	const sampleCount = 64
	overlaps := 0
	for sample := range sampleCount {
		entry := entries[sample*(len(entries)-1)/(sampleCount-1)]
		if entry.pathMetrics.mayMatch(probe) {
			overlaps++
			if overlaps > 8 {
				return nil
			}
		}
	}

	candidates := make([]int, 0, len(entries))
	boundedCount := 0
	for i, entry := range entries {
		metrics := entry.pathMetrics
		// Invalid bounds cannot match any path, and empty delete files cannot
		// contribute positions. The ordinary suffix scan makes the same choice.
		if metrics.skip || (metrics.hasUpper && metrics.upper < metrics.lower) {
			continue
		}
		candidates = append(candidates, i)
		if metrics.hasUpper {
			boundedCount++
		}
	}
	if len(candidates) < positionalPathRangeMinEntries ||
		boundedCount*100 < len(candidates)*positionalPathRangeMinBoundedPercent {
		return nil
	}

	// Sort by lower bound rather than by sequence. A balanced tree over this
	// order supports range pruning; original sequence order is recovered after
	// collecting the (normally very small) matching set.
	slices.SortStableFunc(candidates, func(a, b int) int {
		return cmp.Compare(entries[a].pathMetrics.lower, entries[b].pathMetrics.lower)
	})

	// Before retaining an index, estimate its worst overlapping region with a
	// sweep of sorted interval ends. Very dense bounds make tree traversal and
	// sorting matches more expensive than the existing sequence-suffix walk.
	ends := make([]string, 0, boundedCount)
	for _, i := range candidates {
		if metrics := entries[i].pathMetrics; metrics.hasUpper {
			ends = append(ends, metrics.upper)
		}
	}
	slices.Sort(ends)
	active, ended := 0, 0
	maxUsefulOverlap := max(16, len(candidates)/32)
	for _, i := range candidates {
		lower := entries[i].pathMetrics.lower
		for ended < len(ends) && ends[ended] < lower {
			active--
			ended++
		}
		active++
		if active > maxUsefulOverlap {
			return nil
		}
	}

	idx := &positionalDeletePathRangeIndex{
		nodes: make([]positionalDeletePathRangeNode, 0, len(candidates)),
	}
	idx.root = idx.build(candidates, entries)

	return idx
}

func (idx *positionalDeletePathRangeIndex) build(
	order []int, entries []positionalPartitionDeleteEntry,
) int {
	if len(order) == 0 {
		return -1
	}

	middle := len(order) / 2
	entryIndex := order[middle]
	entry := entries[entryIndex]
	metrics := entry.pathMetrics
	current := len(idx.nodes)
	idx.nodes = append(idx.nodes, positionalDeletePathRangeNode{
		entryIndex:     entryIndex,
		left:           -1,
		right:          -1,
		minLower:       metrics.lower,
		maxUpper:       metrics.upper,
		unboundedUpper: !metrics.hasUpper,
		maxSequence:    entry.entry.DataSequenceNumber(),
	})

	left := idx.build(order[:middle], entries)
	right := idx.build(order[middle+1:], entries)
	node := &idx.nodes[current]
	node.left, node.right = left, right
	for _, childIndex := range [...]int{left, right} {
		if childIndex < 0 {
			continue
		}
		child := idx.nodes[childIndex]
		if child.minLower < node.minLower {
			node.minLower = child.minLower
		}
		if child.maxUpper > node.maxUpper {
			node.maxUpper = child.maxUpper
		}
		if child.maxSequence > node.maxSequence {
			node.maxSequence = child.maxSequence
		}
		node.unboundedUpper = node.unboundedUpper || child.unboundedUpper
	}

	return current
}

// collect appends indexes of sequence-eligible path matches. It stops when
// the candidate count reaches limit, letting the caller switch to the cheaper
// linear suffix scan instead of sorting a very large result.
func (idx *positionalDeletePathRangeIndex) collect(
	nodeIndex int, entries []positionalPartitionDeleteEntry,
	sequence int64, path string, matches *[]int, limit int,
) bool {
	if nodeIndex < 0 {
		return true
	}

	node := idx.nodes[nodeIndex]
	if node.maxSequence < sequence || path < node.minLower ||
		(!node.unboundedUpper && path > node.maxUpper) {
		return true
	}

	if !idx.collect(node.left, entries, sequence, path, matches, limit) {
		return false
	}

	entry := entries[node.entryIndex]
	if entry.entry.DataSequenceNumber() >= sequence && entry.pathMetrics.mayMatch(path) {
		*matches = append(*matches, node.entryIndex)
		if len(*matches) >= limit {
			return false
		}
	}

	return idx.collect(node.right, entries, sequence, path, matches, limit)
}

func appendPartitionDeletesFromRangeIndex(
	out []iceberg.DataFile,
	entries []positionalPartitionDeleteEntry,
	idx *positionalDeletePathRangeIndex,
	sequence int64, path string,
) ([]iceberg.DataFile, error) {
	if idx == nil {
		return appendPartitionDeletesFromSequence(out, entries, sequence, path)
	}

	// The usual lookup returns zero or a few matches. Keep the corresponding
	// index list on the stack and allocate only if the match count grows.
	var inline [16]int
	matches := inline[:0]
	if !idx.collect(idx.root, entries, sequence, path, &matches, max(16, len(entries)/64)) {
		return appendPartitionDeletesFromSequence(out, entries, sequence, path)
	}

	// Matches were found in path order; return the original stable sequence
	// order, even for identical sequence numbers and mixed path bound types.
	slices.Sort(matches)
	for _, entryIndex := range matches {
		out = append(out, entries[entryIndex].entry.DataFile())
	}

	return out, nil
}

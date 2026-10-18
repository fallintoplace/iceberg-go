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
	"cmp"
	"slices"
	"sort"
	"sync"

	"github.com/apache/iceberg-go"
)

const (
	// Small buckets and short sequence suffixes are cheaper to scan directly.
	positionalDeleteRangeMinBucketEntries = 256
	positionalDeleteRangeMinLookupEntries = 128
	positionalDeleteRangeCandidateLimit   = 128
)

// positionalDeleteRangeIndex skips disjoint file_path ranges when a partition
// bucket contains many multi-target position deletes. Entries are already
// sequence-sorted by buildPositionalDeleteIndex. The index is built only when
// a data-file lookup needs it, so unvisited partitions incur no tree cost.
// The tree and underlying metadata are immutable after once.Do completes.
type positionalDeleteRangeIndex struct {
	entries  []iceberg.ManifestEntry
	once     sync.Once
	nodes    []positionalDeleteRangeNode
	root     int
	fallback []int // missing/malformed bounds, in original sequence order
}

type positionalDeleteRangeNode struct {
	entryIndex             int
	left, right            int
	lower, upper, maxUpper []byte
	sequence, maxSequence  int64
}

type positionalDeleteRangeItem struct {
	entryIndex int
	lower      []byte
	upper      []byte
	sequence   int64
}

func newPositionalDeleteRangeIndex(entries []iceberg.ManifestEntry) *positionalDeleteRangeIndex {
	if len(entries) < positionalDeleteRangeMinBucketEntries {
		return nil
	}
	return &positionalDeleteRangeIndex{entries: entries, root: -1}
}

func (idx *positionalDeleteRangeIndex) build() {
	items := make([]positionalDeleteRangeItem, 0, len(idx.entries))
	for i, entry := range idx.entries {
		file := entry.DataFile()
		// Empty delete files never match, independently of file_path.
		if file.Count() == 0 {
			continue
		}
		_, _, _, lowers, uppers := dataFileStats(file)
		lower, upper := lowers[filePathFieldID], uppers[filePathFieldID]
		if lower == nil || upper == nil || bytes.Compare(lower, upper) > 0 {
			idx.fallback = append(idx.fallback, i)
			continue
		}
		items = append(items, positionalDeleteRangeItem{
			entryIndex: i, lower: lower, upper: upper, sequence: entry.SequenceNum(),
		})
	}

	// Uncertain metadata or very few usable ranges cannot amortize a tree.
	if len(items) < positionalDeleteRangeMinLookupEntries || len(idx.fallback) >= len(idx.entries)/2 {
		idx.fallback = nil
		return
	}

	slices.SortFunc(items, func(a, b positionalDeleteRangeItem) int {
		if order := bytes.Compare(a.lower, b.lower); order != 0 {
			return order
		}
		return cmp.Compare(a.entryIndex, b.entryIndex)
	})
	idx.nodes = make([]positionalDeleteRangeNode, len(items))
	var makeSubtree func(int, int) int
	makeSubtree = func(start, end int) int {
		if start >= end {
			return -1
		}
		middle := start + (end-start)/2
		item := items[middle]
		left, right := makeSubtree(start, middle), makeSubtree(middle+1, end)
		node := positionalDeleteRangeNode{
			entryIndex: item.entryIndex,
			left:       left, right: right,
			lower: item.lower, upper: item.upper, maxUpper: item.upper,
			sequence: item.sequence, maxSequence: item.sequence,
		}
		for _, childIndex := range [...]int{left, right} {
			if childIndex < 0 {
				continue
			}
			child := &idx.nodes[childIndex]
			if bytes.Compare(child.maxUpper, node.maxUpper) > 0 {
				node.maxUpper = child.maxUpper
			}
			if child.maxSequence > node.maxSequence {
				node.maxSequence = child.maxSequence
			}
		}
		idx.nodes[middle] = node
		return middle
	}
	idx.root = makeSubtree(0, len(items))
}

// candidatesInPathRange writes into caller-owned bounded storage. A false
// result signals that matching is too dense for indexing to help.
func (idx *positionalDeleteRangeIndex) candidatesInPathRange(
	nodeIndex int, path []byte, sequence int64, candidates []int, count int,
) (int, bool) {
	if nodeIndex < 0 {
		return count, true
	}
	node := &idx.nodes[nodeIndex]
	if node.maxSequence < sequence || bytes.Compare(node.maxUpper, path) < 0 {
		return count, true
	}
	var ok bool
	count, ok = idx.candidatesInPathRange(node.left, path, sequence, candidates, count)
	if !ok {
		return count, false
	}
	if bytes.Compare(node.lower, path) > 0 {
		// Every node in the right subtree has an even larger lower bound.
		return count, true
	}
	if node.sequence >= sequence && bytes.Compare(node.upper, path) >= 0 {
		candidates[count] = node.entryIndex
		count++
		if count == len(candidates) {
			return count, false
		}
	}
	return idx.candidatesInPathRange(node.right, path, sequence, candidates, count)
}

func (idx *positionalDeleteRangeIndex) appendMatches(
	out []iceberg.DataFile, sequence int64, filePath string,
) ([]iceberg.DataFile, error) {
	start := sort.Search(len(idx.entries), func(i int) bool {
		return idx.entries[i].SequenceNum() >= sequence
	})
	remaining := len(idx.entries) - start
	if remaining < positionalDeleteRangeMinLookupEntries {
		return appendPartitionDeletesFromSequence(out, idx.entries, sequence, filePath)
	}
	idx.once.Do(idx.build)
	if idx.root < 0 {
		return appendPartitionDeletesFromSequence(out, idx.entries, sequence, filePath)
	}

	fallbackStart := sort.Search(len(idx.fallback), func(i int) bool {
		return idx.fallback[i] >= start
	})
	fallback := idx.fallback[fallbackStart:]
	if len(fallback) >= remaining/2 {
		return appendPartitionDeletesFromSequence(out, idx.entries, sequence, filePath)
	}

	var candidateBuffer [positionalDeleteRangeCandidateLimit]int
	limit := min(positionalDeleteRangeCandidateLimit, remaining/2)
	count, ok := idx.candidatesInPathRange(idx.root, []byte(filePath), sequence, candidateBuffer[:limit], 0)
	if !ok {
		return appendPartitionDeletesFromSequence(out, idx.entries, sequence, filePath)
	}
	candidates := candidateBuffer[:count]
	slices.Sort(candidates) // Restore sequence-stable order, including ties.

	// Merge uncertain entries and the shortlist by their sequence-sorted
	// indices. Recheck the existing predicate for exact metadata semantics.
	i, j := 0, 0
	for i < len(candidates) || j < len(fallback) {
		var entryIndex int
		if i < len(candidates) && (j == len(fallback) || candidates[i] < fallback[j]) {
			entryIndex = candidates[i]
			i++
		} else {
			entryIndex = fallback[j]
			j++
		}
		file := idx.entries[entryIndex].DataFile()
		if filePathMayMatch(file, filePath) {
			out = append(out, file)
		}
	}
	return out, nil
}

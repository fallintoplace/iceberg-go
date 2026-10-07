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
	"fmt"
	"slices"
	"sort"

	"github.com/apache/iceberg-go"
)

// positionalDeleteIndex groups file-scoped deletes by referenced data path
// and all remaining deletes by partition. Each bucket is sequence-sorted so a
// data-file lookup only visits deletes that can apply to that file.
type positionalDeleteIndex struct {
	byPath      map[string][]iceberg.ManifestEntry
	byPartition map[string]*positionalDeletePartitionBucket
}

type positionalDeletePartitionBucket struct {
	entries         []iceberg.ManifestEntry
	pathRanges      []positionalDeletePathRange
	fallbackIndexes []int
}

type positionalDeletePathRange struct {
	lower        string
	upper        string
	entryIndexes []int
}

type positionalDeleteIndexedRange struct {
	lower      string
	upper      string
	entryIndex int
}

func buildPositionalDeleteIndex(entries []iceberg.ManifestEntry) (*positionalDeleteIndex, error) {
	idx := &positionalDeleteIndex{}
	var partitionEntries map[string][]iceberg.ManifestEntry
	for _, entry := range entries {
		deleteFile := entry.DataFile()
		if path := referencedDataFilePath(deleteFile); path != "" {
			if idx.byPath == nil {
				idx.byPath = make(map[string][]iceberg.ManifestEntry)
			}
			idx.byPath[path] = append(idx.byPath[path], entry)

			continue
		}

		partitionKey, err := canonicalPartitionKey(deleteFile.SpecID(), dataFilePartition(deleteFile))
		if err != nil {
			return nil, fmt.Errorf("indexing positional delete file %s: %w", deleteFile.FilePath(), err)
		}
		if partitionEntries == nil {
			partitionEntries = make(map[string][]iceberg.ManifestEntry)
		}
		partitionEntries[partitionKey] = append(partitionEntries[partitionKey], entry)
	}

	sortBySequence := func(entries []iceberg.ManifestEntry) {
		slices.SortStableFunc(entries, func(a, b iceberg.ManifestEntry) int {
			return cmp.Compare(a.SequenceNum(), b.SequenceNum())
		})
	}
	for _, pathEntries := range idx.byPath {
		sortBySequence(pathEntries)
	}
	if len(partitionEntries) > 0 {
		idx.byPartition = make(map[string]*positionalDeletePartitionBucket, len(partitionEntries))
		for partitionKey, entries := range partitionEntries {
			sortBySequence(entries)
			idx.byPartition[partitionKey] = buildPositionalDeletePartitionBucket(entries)
		}
	}

	return idx, nil
}

func buildPositionalDeletePartitionBucket(entries []iceberg.ManifestEntry) *positionalDeletePartitionBucket {
	bucket := &positionalDeletePartitionBucket{entries: entries}
	indexedRanges := make([]positionalDeleteIndexedRange, 0, len(entries))
	fallbackIndexes := make([]int, 0, len(entries))
	for entryIndex, entry := range entries {
		lower, upper, ok := positionalDeleteFilePathRange(entry.DataFile())
		if !ok {
			fallbackIndexes = append(fallbackIndexes, entryIndex)

			continue
		}
		indexedRanges = append(indexedRanges, positionalDeleteIndexedRange{
			lower: lower, upper: upper, entryIndex: entryIndex,
		})
	}
	if len(indexedRanges) < 2 {
		return bucket
	}

	slices.SortStableFunc(indexedRanges, func(a, b positionalDeleteIndexedRange) int {
		if byLower := cmp.Compare(a.lower, b.lower); byLower != 0 {
			return byLower
		}

		return cmp.Compare(a.upper, b.upper)
	})

	componentForEntry := make([]int, len(entries))
	for i := range componentForEntry {
		componentForEntry[i] = -1
	}
	components := make([]positionalDeletePathRange, 0, len(indexedRanges))
	for _, indexed := range indexedRanges {
		last := len(components) - 1
		if last < 0 || indexed.lower > components[last].upper {
			components = append(components, positionalDeletePathRange{
				lower: indexed.lower,
				upper: indexed.upper,
			})
			last++
		} else if indexed.upper > components[last].upper {
			components[last].upper = indexed.upper
		}
		componentForEntry[indexed.entryIndex] = last
	}

	// A single component cannot narrow candidate lookups within its envelope,
	// so keep the existing sequence-suffix scan for dense overlapping ranges.
	if len(components) < 2 {
		return bucket
	}

	for entryIndex, componentIndex := range componentForEntry {
		if componentIndex >= 0 {
			components[componentIndex].entryIndexes = append(
				components[componentIndex].entryIndexes, entryIndex)
		}
	}
	bucket.pathRanges = components
	bucket.fallbackIndexes = fallbackIndexes

	return bucket
}

func positionalDeleteFilePathRange(deleteFile iceberg.DataFile) (string, string, bool) {
	_, _, _, lowerBounds, upperBounds := dataFileStats(deleteFile)
	lower := lowerBounds[filePathFieldID]
	upper := upperBounds[filePathFieldID]
	if lower == nil || upper == nil || bytes.Compare(lower, upper) > 0 {
		return "", "", false
	}

	return string(lower), string(upper), true
}

// forDataFile returns positional deletes with a greater than or equal sequence
// number. Partition-scoped candidates are pruned using file_path metrics and
// returned before path-scoped deletes, matching Java's ordering.
func (idx *positionalDeleteIndex) forDataFile(dataEntry iceberg.ManifestEntry) ([]iceberg.DataFile, error) {
	if len(idx.byPath) == 0 && len(idx.byPartition) == 0 {
		return nil, nil
	}

	dataFile := dataEntry.DataFile()
	var partitionBucket *positionalDeletePartitionBucket
	if len(idx.byPartition) > 0 {
		partitionKey, err := canonicalPartitionKey(dataFile.SpecID(), dataFilePartition(dataFile))
		if err != nil {
			return nil, fmt.Errorf("matching positional deletes to data file %s: %w", dataFile.FilePath(), err)
		}
		partitionBucket = idx.byPartition[partitionKey]
	}

	dataSeqNum := dataEntry.SequenceNum()
	out, err := appendPartitionBucketDeletesFromSequence(
		nil, partitionBucket, dataSeqNum, dataFile.FilePath())
	if err != nil {
		return nil, err
	}
	out = appendPositionalDeletesFromSequence(
		out, idx.byPath[dataFile.FilePath()], dataSeqNum)

	return out, nil
}

func appendPartitionBucketDeletesFromSequence(
	out []iceberg.DataFile,
	bucket *positionalDeletePartitionBucket,
	dataSeqNum int64,
	dataFilePath string,
) ([]iceberg.DataFile, error) {
	if bucket == nil {
		return out, nil
	}
	if len(bucket.pathRanges) == 0 {
		return appendPartitionDeletesFromSequence(out, bucket.entries, dataSeqNum, dataFilePath)
	}

	componentIndex := sort.Search(len(bucket.pathRanges), func(i int) bool {
		return bucket.pathRanges[i].upper >= dataFilePath
	})
	var componentEntries []int
	if componentIndex < len(bucket.pathRanges) &&
		bucket.pathRanges[componentIndex].lower <= dataFilePath {
		componentEntries = bucket.pathRanges[componentIndex].entryIndexes
	}

	componentStart := partitionEntryIndexStart(bucket.entries, componentEntries, dataSeqNum)
	fallbackStart := partitionEntryIndexStart(bucket.entries, bucket.fallbackIndexes, dataSeqNum)
	componentEntries = componentEntries[componentStart:]
	fallbackEntries := bucket.fallbackIndexes[fallbackStart:]

	for len(componentEntries) > 0 || len(fallbackEntries) > 0 {
		var entryIndex int
		switch {
		case len(fallbackEntries) == 0:
			entryIndex = componentEntries[0]
			componentEntries = componentEntries[1:]
		case len(componentEntries) == 0:
			entryIndex = fallbackEntries[0]
			fallbackEntries = fallbackEntries[1:]
		case componentEntries[0] < fallbackEntries[0]:
			entryIndex = componentEntries[0]
			componentEntries = componentEntries[1:]
		default:
			entryIndex = fallbackEntries[0]
			fallbackEntries = fallbackEntries[1:]
		}

		deleteFile := bucket.entries[entryIndex].DataFile()
		if filePathMayMatch(deleteFile, dataFilePath) {
			out = append(out, deleteFile)
		}
	}

	return out, nil
}

func partitionEntryIndexStart(
	entries []iceberg.ManifestEntry,
	entryIndexes []int,
	dataSeqNum int64,
) int {
	return sort.Search(len(entryIndexes), func(i int) bool {
		return entries[entryIndexes[i]].SequenceNum() >= dataSeqNum
	})
}

func appendPartitionDeletesFromSequence(
	out []iceberg.DataFile,
	entries []iceberg.ManifestEntry,
	dataSeqNum int64,
	dataFilePath string,
) ([]iceberg.DataFile, error) {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].SequenceNum() >= dataSeqNum
	})
	if start == len(entries) {
		return out, nil
	}

	for _, entry := range entries[start:] {
		deleteFile := entry.DataFile()
		if filePathMayMatch(deleteFile, dataFilePath) {
			out = append(out, deleteFile)
		}
	}

	return out, nil
}

// filePathMayMatch mirrors inclusive metrics evaluation for the required
// file_path field in a position-delete file. It only checks the bounds needed
// by the positional-delete index and keeps missing or malformed metadata
// conservative.
func filePathMayMatch(deleteFile iceberg.DataFile, dataFilePath string) bool {
	if deleteFile.Count() == 0 {
		return false
	}

	valueCounts, nullCounts, nanCounts, lowerBounds, upperBounds := dataFileStats(deleteFile)
	if valueCount, ok := valueCounts[filePathFieldID]; ok {
		if nullCount, ok := nullCounts[filePathFieldID]; ok && nullCount == valueCount {
			return false
		}
		if nanCount, ok := nanCounts[filePathFieldID]; ok && nanCount == valueCount {
			return false
		}
	}

	if lower := lowerBounds[filePathFieldID]; lower != nil && bytes.Compare(lower, []byte(dataFilePath)) > 0 {
		return false
	}
	if upper := upperBounds[filePathFieldID]; upper != nil && bytes.Compare(upper, []byte(dataFilePath)) < 0 {
		return false
	}

	return true
}

func appendPositionalDeletesFromSequence(
	out []iceberg.DataFile,
	entries []iceberg.ManifestEntry,
	dataSeqNum int64,
) []iceberg.DataFile {
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].SequenceNum() >= dataSeqNum
	})
	for _, entry := range entries[start:] {
		out = append(out, entry.DataFile())
	}

	return out
}

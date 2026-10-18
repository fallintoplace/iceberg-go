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
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
)

// The reference measures the pre-index sequence-suffix matcher, including
// index construction. Both variants produce exactly the same file matches.
func BenchmarkPositionalDeletePlanningMultiTargetRanges(b *testing.B) {
	for _, tc := range []struct {
		name                          string
		dataFiles, deleteFiles, width int
		missingBoundsPercent          int
	}{
		{"narrow_2k", 2_000, 2_000, 25, 0},
		{"narrow_10k", 10_000, 10_000, 100, 0},
		{"narrow_20k", 20_000, 20_000, 100, 0},
		{"missing_10pct", 10_000, 10_000, 100, 10},
		{"wide_overlap", 2_000, 2_000, 1_900, 0},
		{"small_bucket", 2_000, 64, 50, 0},
	} {
		b.Run(tc.name, func(b *testing.B) {
			dataEntries, deleteEntries, expectedMatches := positionalDeleteRangeBenchmarkEntries(
				tc.dataFiles, tc.deleteFiles, tc.width, tc.missingBoundsPercent)
			partitionKey, err := canonicalPartitionKey(0, nil)
			if err != nil {
				b.Fatal(err)
			}
			for _, variant := range []struct {
				name    string
				indexed bool
			}{{"linear", false}, {"range_index", true}} {
				b.Run(variant.name, func(b *testing.B) {
					b.ReportAllocs()
					b.ReportMetric(float64(tc.dataFiles), "data_files")
					b.ReportMetric(float64(tc.deleteFiles), "delete_files")
					b.ResetTimer()
					var matched int
					for range b.N {
						idx, err := buildPositionalDeleteIndex(deleteEntries)
						if err != nil {
							b.Fatal(err)
						}
						matched = 0
						for _, dataEntry := range dataEntries {
							var matchedFiles []iceberg.DataFile
							if variant.indexed {
								matchedFiles, err = idx.forDataFile(dataEntry)
							} else {
								matchedFiles, err = appendPartitionDeletesFromSequence(nil,
									idx.byPartition[partitionKey], dataEntry.SequenceNum(), dataEntry.DataFile().FilePath())
							}
							if err != nil {
								b.Fatal(err)
							}
							matched += len(matchedFiles)
						}
					}
					if matched != expectedMatches {
						b.Fatalf("expected %d matches, got %d", expectedMatches, matched)
					}
				})
			}
		})
	}
}

func positionalDeleteRangeBenchmarkEntries(
	dataCount, deleteCount, width, missingBoundsPercent int,
) ([]iceberg.ManifestEntry, []iceberg.ManifestEntry, int) {
	dataEntries := positionalDeleteBenchmarkDataEntries(dataCount)
	deleteEntries := make([]iceberg.ManifestEntry, deleteCount)
	expectedMatches := 0
	for i := range deleteEntries {
		start := i * (dataCount - 1) / deleteCount
		end := min(dataCount-1, start+width-1)
		lower, upper := positionalDeleteBenchmarkPath(start), positionalDeleteBenchmarkPath(end)
		if missingBoundsPercent > 0 && i%100 < missingBoundsPercent {
			lower, upper = "", ""
			expectedMatches += dataCount
		} else {
			expectedMatches += end - start + 1
		}
		deleteEntries[i] = newPositionalDeleteIndexTestEntryWithBounds(
			fmt.Sprintf("delete-%06d.parquet", i), 0, nil, 2, nil, lower, upper)
	}
	return dataEntries, deleteEntries, expectedMatches
}

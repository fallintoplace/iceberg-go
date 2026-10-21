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
	"context"
	"fmt"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/require"
)

// benchmarkSerialSurvivorManifestScan is the pre-change implementation.
// Keeping it in the benchmark gives a like-for-like comparison on identical
// real Avro manifests and the same FileIO, rather than a synthetic scheduler.
func benchmarkSerialSurvivorManifestScan(
	ctx context.Context, fs iceio.IO, manifests []iceberg.ManifestFile,
	rewrittenPaths map[string]struct{},
) (map[string]int64, error) {
	minSeq := make(map[string]int64)
	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if manifest.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		for entry, err := range manifest.Entries(fs, true) {
			if err != nil {
				return nil, err
			}
			df := entry.DataFile()
			if df.ContentType() != iceberg.EntryContentData {
				continue
			}
			if _, rewritten := rewrittenPaths[df.FilePath()]; rewritten {
				continue
			}

			seq := max(entry.SequenceNum(), 0)
			key, err := canonicalPartitionKey(df.SpecID(), dataFilePartition(df))
			if err != nil {
				return nil, err
			}
			if current, found := minSeq[key]; found {
				seq = min(seq, current)
			}
			minSeq[key] = seq
		}
	}

	return minSeq, nil
}

// BenchmarkDeadPositionDeleteSurvivorManifests includes real Avro manifest
// decoding and a 10ms FileIO latency per manifest, modeling remote storage.
// Compare serial and bounded-parallel scans at different manifest counts.
func BenchmarkDeadPositionDeleteSurvivorManifests(b *testing.B) {
	for _, manifestCount := range []int{1, 8, 32} {
		b.Run(fmt.Sprintf("manifests=%d", manifestCount), func(b *testing.B) {
			tbl := benchmarkInspectPartitionTable(b, manifestCount)
			fs, err := tbl.FS(b.Context())
			require.NoError(b, err)
			manifests, err := tbl.CurrentSnapshot().Manifests(fs)
			require.NoError(b, err)
			rewritten := map[string]struct{}{}

			serial, err := benchmarkSerialSurvivorManifestScan(b.Context(), fs, manifests, rewritten)
			require.NoError(b, err)
			parallel, err := minSurvivorSeqByPartition(b.Context(), fs, manifests, rewritten)
			require.NoError(b, err)
			require.Equal(b, serial, parallel)

			b.Run("serial", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := benchmarkSerialSurvivorManifestScan(b.Context(), fs, manifests, rewritten); err != nil {
						b.Fatal(err)
					}
				}
			})
			b.Run("parallel", func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := minSurvivorSeqByPartition(b.Context(), fs, manifests, rewritten); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

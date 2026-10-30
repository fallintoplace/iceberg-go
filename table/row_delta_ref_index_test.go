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
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
)

func newRefIndexTestDV(t testing.TB, ref string) iceberg.DataFile {
	t.Helper()

	builder, err := iceberg.NewDataFileBuilder(
		*iceberg.UnpartitionedSpec, iceberg.EntryContentPosDeletes,
		"mem://test/shared.puffin", iceberg.PuffinFile, nil, nil, nil, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	if ref != "" {
		builder.ReferencedDataFile(ref)
	}

	return builder.Build()
}

func TestIndexLiveDeleteRefs(t *testing.T) {
	first := newRefIndexTestDV(t, "data-a.parquet")
	second := newRefIndexTestDV(t, "data-b.parquet")
	duplicate := newRefIndexTestDV(t, "data-a.parquet")
	unset := newRefIndexTestDV(t, "")
	index := indexLiveDeleteRefs([]iceberg.DataFile{
		first, second, duplicate, unset,
	})

	if match := index["data-a.parquet"]; match.count != 2 || match.file != first {
		t.Fatalf("duplicate ref: got %#v, want first entry and count 2", match)
	}
	if match := index["data-b.parquet"]; match.count != 1 || match.file != second {
		t.Fatalf("unique ref: got %#v, want second entry and count 1", match)
	}
	if match := index[""]; match.count != 1 || match.file != unset {
		t.Fatalf("unset ref: got %#v, want unset entry and count 1", match)
	}
	if match := index["missing"]; match.count != 0 || match.file != nil {
		t.Fatalf("missing ref: got %#v, want no match", match)
	}
}

func TestIndexLiveDeleteRefsMatchesLinearSearch(t *testing.T) {
	for _, size := range []int{0, 1, 2, 17, 257} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			candidates := make([]iceberg.DataFile, size)
			for i := range candidates {
				candidates[i] = newRefIndexTestDV(t, "data-"+strconv.Itoa(i%11))
			}
			index := indexLiveDeleteRefs(candidates)

			for i := range 12 {
				ref := "data-" + strconv.Itoa(i)
				var first iceberg.DataFile
				count := 0
				for _, candidate := range candidates {
					if explicitReferencedDataFile(candidate) == ref {
						if count == 0 {
							first = candidate
						}
						count++
					}
				}
				match := index[ref]
				if match.file != first || match.count != count {
					t.Fatalf("ref %s: index (%v, %d), linear (%v, %d)",
						ref, match.file, match.count, first, count)
				}
			}
		})
	}
}

var benchmarkSharedPuffinLookupSink int

// BenchmarkSharedPuffinRefLookup models N live entries and N removals at
// one Puffin path. The linear case is the previous repeated candidate scan.
func BenchmarkSharedPuffinRefLookup(b *testing.B) {
	for _, count := range []int{128, 1024, 4096} {
		candidates := make([]iceberg.DataFile, count)
		refs := make([]string, count)
		for i := range count {
			ref := "data/file-" + strconv.Itoa(i) + ".parquet"
			candidates[i] = newRefIndexTestDV(b, ref)
			refs[i] = ref
		}
		requests := make([]string, count)
		for i := range requests {
			requests[i] = refs[(i*7919)%count]
		}

		b.Run(fmt.Sprintf("linear_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				total := 0
				for _, ref := range requests {
					matches := 0
					for _, candidate := range candidates {
						if explicitReferencedDataFile(candidate) == ref {
							matches++
						}
					}
					if matches != 1 {
						b.Fatalf("ref %q: got %d matches", ref, matches)
					}
					total += matches
				}
				benchmarkSharedPuffinLookupSink = total
			}
		})
		b.Run(fmt.Sprintf("indexed_%d", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				index := indexLiveDeleteRefs(candidates)
				total := 0
				for _, ref := range requests {
					match := index[ref]
					if match.count != 1 {
						b.Fatalf("ref %q: got %d matches", ref, match.count)
					}
					total += match.count
				}
				benchmarkSharedPuffinLookupSink = total
			}
		})
	}
}

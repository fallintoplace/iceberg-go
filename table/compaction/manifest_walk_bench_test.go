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
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

var benchmarkManifestVisits atomic.Int64

func BenchmarkCompactionManifestWalk(b *testing.B) {
	for _, n := range []int{16, 64, 256} {
		for _, latency := range []time.Duration{0, time.Millisecond} {
			input := make([]int, n)
			for i := range input {
				input[i] = i
			}
			prefix := fmt.Sprintf("N=%d/latency=%s", n, latency)
			b.Run(prefix+"/serial", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					for _, value := range input {
						if latency > 0 {
							time.Sleep(latency)
						}
						benchmarkManifestVisits.Add(int64(value))
					}
				}
			})
			b.Run(prefix+"/parallel8", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					err := walkCompactionManifests(context.Background(), input, func(_ context.Context, _ int, _ int, value int) error {
						if latency > 0 {
							time.Sleep(latency)
						}
						benchmarkManifestVisits.Add(int64(value))
						return nil
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

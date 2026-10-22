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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iceberg

import (
	"math"
	"math/rand"
	"testing"
	"time"
)

func referenceDayPartitionFormat(days int32) string {
	return epochTM.AddDate(0, 0, int(days)).Format("2006-01-02")
}

func referenceHourPartitionFormat(hours int32) string {
	return time.Unix(int64(hours)*int64(time.Hour/time.Second), 0).UTC().Format("2006-01-02-15")
}

func TestDayHourPartitionFormattingParity(t *testing.T) {
	cases := []int32{
		math.MinInt32, math.MinInt32 + 1,
		-1460970, -719469, -719468, -719467, -146097,
		-10000, -366, -365, -60, -1,
		0, 1, 58, 59, 60, 365, 366, 18628, 19782, 20000,
		146097, 1460970, math.MaxInt32 - 1, math.MaxInt32,
	}
	check := func(n int32) {
		t.Helper()
		if got, want := (DayTransform{}).ToHumanStr(n), referenceDayPartitionFormat(n); got != want {
			t.Fatalf("day %d: got %q, want %q", n, got, want)
		}
		if got, want := (DayTransform{}).ToHumanStr(Date(n)), referenceDayPartitionFormat(n); got != want {
			t.Fatalf("Date %d: got %q, want %q", n, got, want)
		}
		if got, want := (HourTransform{}).ToHumanStr(n), referenceHourPartitionFormat(n); got != want {
			t.Fatalf("hour %d: got %q, want %q", n, got, want)
		}
	}
	for _, n := range cases {
		check(n)
	}

	// Covers leap years, century boundaries, negative years and large years.
	rng := rand.New(rand.NewSource(19700101))
	for range 100000 {
		check(int32(rng.Uint32()))
	}

	if got := (DayTransform{}).ToHumanStr(nil); got != "null" {
		t.Fatalf("day nil: %q", got)
	}
	if got := (HourTransform{}).ToHumanStr(nil); got != "null" {
		t.Fatalf("hour nil: %q", got)
	}
	if got := (HourTransform{}).ToHumanStr(int64(1)); got != "null" {
		t.Fatalf("hour wrong type: %q", got)
	}
}

var dayHourFormatBenchmarkSink string

func BenchmarkDayHourPartitionFormatting(b *testing.B) {
	for _, tc := range []struct {
		name       string
		values     []int32
		reference  func(int32) string
		optimized  func(int32) string
	}{
		{
			"day/recent",
			[]int32{0, 18628, 19782, 20000, 21000},
			referenceDayPartitionFormat,
			func(n int32) string { return (DayTransform{}).ToHumanStr(n) },
		},
		{
			"day/pre-epoch",
			[]int32{-1, -365, -7305, -719468, -146097},
			referenceDayPartitionFormat,
			func(n int32) string { return (DayTransform{}).ToHumanStr(n) },
		},
		{
			"day/extremes",
			[]int32{math.MinInt32, math.MinInt32 + 1, 0, math.MaxInt32 - 1, math.MaxInt32},
			referenceDayPartitionFormat,
			func(n int32) string { return (DayTransform{}).ToHumanStr(n) },
		},
		{
			"hour/recent",
			[]int32{0, 458000, 470000, 490000, 500000},
			referenceHourPartitionFormat,
			func(n int32) string { return (HourTransform{}).ToHumanStr(n) },
		},
		{
			"hour/pre-epoch",
			[]int32{-1, -24, -8784, -100000, -280000},
			referenceHourPartitionFormat,
			func(n int32) string { return (HourTransform{}).ToHumanStr(n) },
		},
		{
			"hour/extremes",
			[]int32{math.MinInt32, math.MinInt32 + 1, 0, math.MaxInt32 - 1, math.MaxInt32},
			referenceHourPartitionFormat,
			func(n int32) string { return (HourTransform{}).ToHumanStr(n) },
		},
	} {
		for _, impl := range []struct {
			name string
			fn   func(int32) string
		}{
			{"before", tc.reference},
			{"after", tc.optimized},
		} {
			b.Run(tc.name+"/"+impl.name, func(b *testing.B) {
				b.ReportAllocs()
				var out string
				for i := range b.N {
					out = impl.fn(tc.values[i%len(tc.values)])
				}
				dayHourFormatBenchmarkSink = out
			})
		}
	}
}

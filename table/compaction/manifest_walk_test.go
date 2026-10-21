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
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func TestWalkCompactionManifestsExactOnce(t *testing.T) {
	for _, count := range []int{0, 1, 2, 7, 8, 9, 127} {
		manifests := make([]int, count)
		for i := range manifests {
			manifests[i] = i
		}
		hits := make([]atomic.Int32, count)
		err := walkCompactionManifests(context.Background(), manifests, func(_ context.Context, worker, index, value int) error {
			if worker < 0 || worker >= compactionManifestWorkerCount(count) {
				t.Errorf("worker index %d", worker)
			}
			if index != value {
				t.Errorf("manifest order value mismatch index=%d value=%d", index, value)
			}
			hits[index].Add(1)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		for i := range hits {
			if got := hits[i].Load(); got != 1 {
				t.Errorf("count=%d index=%d hits=%d", count, i, got)
			}
		}
	}
}

func TestWalkCompactionManifestsError(t *testing.T) {
	sentinel := errors.New("broken manifest")
	manifests := make([]int, 64)
	err := walkCompactionManifests(context.Background(), manifests, func(_ context.Context, _ int, index int, _ int) error {
		if index == 3 {
			return sentinel
		}
		return nil
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want sentinel", err)
	}
}

func TestWalkCompactionManifestsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := walkCompactionManifests(ctx, make([]int, 20), func(context.Context, int, int, int) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want canceled", err)
	}
}

func TestWalkCompactionManifestsWorkerMax(t *testing.T) {
	if got := compactionManifestWorkerCount(1000); got != 8 {
		t.Fatalf("got %d", got)
	}
	var active, peak atomic.Int32
	manifests := make([]int, 64)
	err := walkCompactionManifests(context.Background(), manifests, func(_ context.Context, _, _ int, _ int) error {
		cur := active.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(100 * time.Microsecond)
		active.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := peak.Load(); got > 8 {
		t.Fatalf("got peak %d", got)
	}
}

func TestWalkCompactionManifestsOrderedResults(t *testing.T) {
	manifests := []int{5, 3, 8, 2, 1, 7}
	results := make([]int, len(manifests))
	err := walkCompactionManifests(context.Background(), manifests, func(_ context.Context, _, i, value int) error {
		time.Sleep(time.Duration(9-value) * time.Millisecond)
		results[i] = value
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(manifests, results) {
		t.Errorf("got %v, want %v", results, manifests)
	}
}

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

package catalog

import (
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
)

func TestRegistryZeroValueAndIsolation(t *testing.T) {
	var first, second registry
	registrar := RegistrarFunc(func(context.Context, string, iceberg.Properties) (Catalog, error) {
		return nil, nil
	})

	first.set("rest", registrar)
	first.set("glue", registrar)
	second.set("sql", registrar)

	if got, want := first.getKeys(), []string{"glue", "rest"}; !slices.Equal(got, want) {
		t.Fatalf("first registry keys = %v, want %v", got, want)
	}
	if got, want := second.getKeys(), []string{"sql"}; !slices.Equal(got, want) {
		t.Fatalf("second registry keys = %v, want %v", got, want)
	}
	if _, ok := first.get("sql"); ok {
		t.Fatal("first registry unexpectedly contains second registry's catalog")
	}
	if _, ok := second.get("rest"); ok {
		t.Fatal("second registry unexpectedly contains first registry's catalog")
	}

	first.remove("rest")
	if _, ok := second.get("sql"); !ok {
		t.Fatal("removing from first registry changed second registry")
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	var reg registry
	registrar := RegistrarFunc(func(context.Context, string, iceberg.Properties) (Catalog, error) {
		return nil, nil
	})

	const workers = 64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			reg.set(name, registrar)
			if _, ok := reg.get(name); !ok {
				t.Errorf("registered catalog %q not found", name)
			}
			_ = reg.getKeys()
		}(strconv.Itoa(i))
	}
	wg.Wait()

	if got := len(reg.getKeys()); got != workers {
		t.Fatalf("registered catalogs = %d, want %d", got, workers)
	}
}

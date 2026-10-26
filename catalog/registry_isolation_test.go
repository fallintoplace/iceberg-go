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

package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/stretchr/testify/require"
)

func TestScopedCatalogRegistriesAreIndependent(t *testing.T) {
	ctx := context.Background()
	first, second := catalog.NewRegistry(), catalog.NewRegistry()

	require.Contains(t, first.Names(), "rest")
	factoryErr := errors.New("scoped factory called")
	require.NoError(t, first.Register("MyCatalog", catalog.RegistrarFunc(
		func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return nil, factoryErr
		},
	)))

	require.Contains(t, first.Names(), "mycatalog")
	require.NotContains(t, second.Names(), "mycatalog")

	props := iceberg.Properties{"type": "MYCATALOG"}
	_, err := first.Load(ctx, "example", props)
	require.ErrorIs(t, err, factoryErr)
	_, err = second.Load(ctx, "example", props)
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)
	_, err = catalog.Load(ctx, "example", props)
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)

	cloned := first.Clone()
	first.Unregister("MYCATALOG")
	_, err = first.Load(ctx, "example", props)
	require.ErrorIs(t, err, catalog.ErrCatalogNotFound)
	_, err = cloned.Load(ctx, "example", props)
	require.ErrorIs(t, err, factoryErr)
}

func TestScopedCatalogRegistryZeroValue(t *testing.T) {
	var reg catalog.Registry
	require.Empty(t, reg.Names())
	require.ErrorIs(t, reg.Register("invalid", nil), iceberg.ErrInvalidArgument)
	require.Empty(t, reg.Names())
	require.NoError(t, reg.Register("valid", catalog.RegistrarFunc(
		func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
			return nil, nil
		},
	)))
	require.Equal(t, []string{"valid"}, reg.Names())

	copy := reg.Clone()
	reg.Unregister("valid")
	require.Empty(t, reg.Names())
	require.Equal(t, []string{"valid"}, copy.Names())
}

func TestScopedCatalogRegistryConcurrentRegistrations(t *testing.T) {
	var reg catalog.Registry
	const workers = 16
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("type-%d", i)
			err := reg.Register(name, catalog.RegistrarFunc(
				func(context.Context, string, iceberg.Properties) (catalog.Catalog, error) {
					return nil, nil
				},
			))
			if err != nil {
				errs <- err
				return
			}
			_ = reg.Names()
			_ = reg.Clone()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, reg.Names(), workers)
}

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

package rest
import (
	"context"
	"errors"
	"net/http"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func (r *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := r.endpoints.check(endpointCreateNamespace); err != nil {
		return err
	}

	if err := r.checkValidNamespace(namespace); err != nil {
		return err
	}

	path, err := endpointCreateNamespace.reqPath()
	if err != nil {
		return err
	}

	_, err = doPost[map[string]any, struct{}](ctx, r.baseURI, path,
		map[string]any{"namespace": namespace, "properties": props}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrNamespaceAlreadyExists,
		})

	return err
}

func (r *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if err := r.endpoints.check(endpointDeleteNamespace); err != nil {
		return err
	}

	if err := r.checkValidNamespace(namespace); err != nil {
		return err
	}

	path, err := endpointDeleteNamespace.reqPath(r.encodeNamespace(namespace))
	if err != nil {
		return err
	}

	_, err = doDelete[struct{}](ctx, r.baseURI, path,
		r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace,
			http.StatusConflict: catalog.ErrNamespaceNotEmpty,
		})

	return err
}

// ListNamespaces lists namespaces under parent. If the server does not advertise
// the list-namespaces endpoint, it returns an empty slice rather than an error.
func (r *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	var allNamespaces []table.Identifier
	pageSize := r.getPageSize(ctx)
	err := paginateIdentifiers(func(pageToken string) ([]table.Identifier, string, error) {
		return r.listNamespacesPage(ctx, parent, pageToken, pageSize)
	}, func(identifier table.Identifier) bool {
		allNamespaces = append(allNamespaces, identifier)

		return true
	})
	if err != nil {
		return nil, err
	}

	return allNamespaces, nil
}

func (r *Catalog) listNamespacesPage(ctx context.Context, parent table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	if len(parent) != 0 {
		if err := r.checkValidNamespace(parent); err != nil {
			return nil, "", err
		}
	}

	// Unsupported listing yields an empty result rather than an error.
	if !r.endpoints.allowed(endpointListNamespaces) {
		return nil, "", nil
	}

	path, err := endpointListNamespaces.reqPath()
	if err != nil {
		return nil, "", err
	}

	uri := r.baseURI.JoinPath(path...)

	v := paginationQueryParams(pageToken, pageSize)
	if len(parent) != 0 {
		v.Set("parent", r.namespaceToQueryParam(parent))
	}
	if len(v) > 0 {
		uri.RawQuery = v.Encode()
	}

	type rsptype struct {
		Namespaces    []table.Identifier `json:"namespaces"`
		NextPageToken string             `json:"next-page-token,omitempty"`
	}

	rsp, err := doGet[rsptype](ctx, uri, []string{}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, "", err
	}

	return rsp.Namespaces, rsp.NextPageToken, nil
}

func (r *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	if err := r.endpoints.check(endpointLoadNamespace); err != nil {
		return nil, err
	}

	if err := r.checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	type nsresponse struct {
		Namespace table.Identifier   `json:"namespace"`
		Props     iceberg.Properties `json:"properties"`
	}

	path, err := endpointLoadNamespace.reqPath(r.encodeNamespace(namespace))
	if err != nil {
		return nil, err
	}

	rsp, err := doGet[nsresponse](ctx, r.baseURI, path,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	return rsp.Props, nil
}

func (r *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
	removals []string, updates iceberg.Properties,
) (catalog.PropertiesUpdateSummary, error) {
	if err := r.endpoints.check(endpointUpdateNamespace); err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	if err := r.checkValidNamespace(namespace); err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	type payload struct {
		Remove  []string           `json:"removals"`
		Updates iceberg.Properties `json:"updates"`
	}

	ns := r.encodeNamespace(namespace)

	path, err := endpointUpdateNamespace.reqPath(ns)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	return doPost[payload, catalog.PropertiesUpdateSummary](ctx, r.baseURI, path,
		payload{Remove: removals, Updates: updates}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
}

func (r *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	if err := r.checkValidNamespace(namespace); err != nil {
		return false, err
	}

	// No HEAD endpoint: fall back to GET (load). If load is unsupported too, its
	// ErrEndpointNotSupported surfaces rather than a bogus "not found".
	if !r.endpoints.contains(endpointNamespaceExists) {
		_, err := r.LoadNamespaceProperties(ctx, namespace)
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchNamespace) {
				return false, nil
			}

			return false, err
		}

		return true, nil
	}

	path, err := endpointNamespaceExists.reqPath(r.encodeNamespace(namespace))
	if err != nil {
		return false, err
	}

	err = doHead(ctx, r.baseURI, path,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

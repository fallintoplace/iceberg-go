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
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/udf"
)

// FunctionCatalog is an optional interface for catalogs that support the
// function (SQL UDF) read endpoints defined by the REST spec: list and load.
// The function endpoints are not part of the spec's assumed default endpoint
// set, so they are only used when the server advertises them: unsupported
// loads fail with ErrEndpointNotSupported and unsupported listings yield no
// results. Callers holding a catalog.Catalog can check for this capability
// via a type assertion:
//
//	if fc, ok := cat.(rest.FunctionCatalog); ok {
//	    fn, err := fc.LoadFunction(ctx, ident)
//	}
type FunctionCatalog interface {
	// ListFunctions returns the function identifiers under a namespace,
	// with the returned identifiers containing the information required
	// to load the function via this catalog.
	ListFunctions(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error]
	// LoadFunction loads a function from the catalog. All overloaded
	// definitions are included in the single metadata response.
	LoadFunction(ctx context.Context, identifier table.Identifier) (*udf.UDF, error)
	// CheckFunctionExists returns if the function exists. The REST spec
	// defines no HEAD endpoint for functions, so existence is checked by
	// loading the function.
	CheckFunctionExists(ctx context.Context, identifier table.Identifier) (bool, error)
}

// ListFunctions returns the function (SQL UDF) identifiers under a
// namespace, with the returned identifiers containing the information
// required to load the function via this catalog. The function endpoints
// are not part of the spec's assumed default endpoint set; if the server
// does not advertise the list-functions endpoint, it yields no functions
// rather than an error.
func (r *Catalog) ListFunctions(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		pageSize := r.getPageSize(ctx)
		err := paginateIdentifiers(func(pageToken string) ([]table.Identifier, string, error) {
			return r.listFunctionsPage(ctx, namespace, pageToken, pageSize)
		}, func(identifier table.Identifier) bool {
			return yield(identifier, nil)
		})
		if err != nil {
			yield(table.Identifier{}, err)
		}
	}
}

func (r *Catalog) listFunctionsPage(ctx context.Context, namespace table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	return listCatalogObjectsPage(r, ctx, endpointListFunctions, namespace, pageToken, pageSize, functionIdentifier)
}

// loadFunctionResponse contains the response from loading a function.
type loadFunctionResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	RawMetadata json.RawMessage `json:"metadata"`
}

// LoadFunction loads a function (SQL UDF) from the catalog. All overloaded
// definitions are included in the single metadata response. The function
// endpoints are not part of the spec's assumed default endpoint set; if the
// server does not advertise the load-function endpoint, LoadFunction fails
// with ErrEndpointNotSupported.
func (r *Catalog) LoadFunction(ctx context.Context, identifier table.Identifier) (*udf.UDF, error) {
	if err := r.endpoints.check(endpointLoadFunction); err != nil {
		return nil, err
	}

	identPath, err := r.functionIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	return r.loadFunction(ctx, identifier, identPath)
}

// loadFunction fetches and parses a function after endpoint negotiation and
// identifier validation, so its ErrNoSuchFunction is always server-reported.
func (r *Catalog) loadFunction(ctx context.Context, identifier table.Identifier, identPath identifierPath) (*udf.UDF, error) {
	path, err := identPath.objectReqPath(endpointLoadFunction)
	if err != nil {
		return nil, err
	}

	rsp, err := doGet[loadFunctionResponse](ctx, r.baseURI, path,
		r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchFunction,
		})
	if err != nil {
		// The spec's load 404 covers both a missing namespace and a missing
		// function; discriminate on the error type so a missing namespace is
		// not reported as a missing function.
		var errRsp errorResponse
		if errors.As(err, &errRsp) && errRsp.Type == "NoSuchNamespaceException" {
			errRsp.wrapping = catalog.ErrNoSuchNamespace

			return nil, errRsp
		}

		return nil, err
	}

	// The spec marks metadata as required; guard so a buggy server yields a
	// clear error instead of a JSON parsing artifact.
	if len(rsp.RawMetadata) == 0 {
		return nil, fmt.Errorf("%w: load function response is missing metadata", ErrRESTError)
	}

	metadata, err := udf.ParseMetadataBytes(rsp.RawMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse function metadata: %w", err)
	}

	return udf.New(identifier, metadata, rsp.MetadataLoc), nil
}

// CheckFunctionExists returns if the function exists. The REST spec defines
// no HEAD endpoint for functions, so existence is checked by loading the
// function; if loading is unsupported, ErrEndpointNotSupported surfaces
// rather than a bogus "not found". The identifier is validated once here, so
// an invalid one surfaces as an error and only a server-reported "not found"
// becomes (false, nil), matching the table and view existence checks.
func (r *Catalog) CheckFunctionExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	if err := r.endpoints.check(endpointLoadFunction); err != nil {
		return false, err
	}

	identPath, err := r.functionIdentifierPath(identifier)
	if err != nil {
		return false, err
	}

	if _, err := r.loadFunction(ctx, identifier, identPath); err != nil {
		if errors.Is(err, catalog.ErrNoSuchFunction) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

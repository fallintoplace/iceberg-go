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

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
)

// ListViews lists the views in a namespace. If the server does not advertise the
// list-views endpoint, it yields no views rather than an error.
func (r *Catalog) ListViews(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		pageSize := r.getPageSize(ctx)
		err := paginateIdentifiers(func(pageToken string) ([]table.Identifier, string, error) {
			return r.listViewsPage(ctx, namespace, pageToken, pageSize)
		}, func(identifier table.Identifier) bool {
			return yield(identifier, nil)
		})
		if err != nil {
			yield(table.Identifier{}, err)
		}
	}
}

func (r *Catalog) listViewsPage(ctx context.Context, namespace table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	return listCatalogObjectsPage(r, ctx, endpointListViews, namespace, pageToken, pageSize, objectIdentifier)
}

func (r *Catalog) DropView(ctx context.Context, identifier table.Identifier) error {
	if err := r.endpoints.check(endpointDeleteView); err != nil {
		return err
	}

	identPath, err := r.viewIdentifierPath(identifier)
	if err != nil {
		return err
	}

	path, err := identPath.objectReqPath(endpointDeleteView)
	if err != nil {
		return err
	}

	_, err = doDelete[struct{}](ctx, r.baseURI, path, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchView})

	return err
}

func (r *Catalog) CheckViewExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	identPath, err := r.viewIdentifierPath(identifier)
	if err != nil {
		return false, err
	}

	// No HEAD endpoint: fall back to GET (load). If load is unsupported too, its
	// ErrEndpointNotSupported surfaces rather than a bogus "not found".
	if !r.endpoints.contains(endpointViewExists) {
		_, err := r.LoadView(ctx, identifier)
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchView) {
				return false, nil
			}

			return false, err
		}

		return true, nil
	}

	path, err := identPath.objectReqPath(endpointViewExists)
	if err != nil {
		return false, err
	}

	err = doHead(ctx, r.baseURI, path,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchView})
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchView) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

type createViewRequest struct {
	Name        string             `json:"name"`
	Schema      *iceberg.Schema    `json:"schema"`
	Location    string             `json:"location,omitempty"`
	Props       iceberg.Properties `json:"properties"`
	ViewVersion *view.Version      `json:"view-version"`
}

type viewResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	RawMetadata json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
	Labels      *iceberg.Labels    `json:"labels,omitempty"`
	Metadata    view.Metadata      `json:"-"`
}

func (v *viewResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias viewResponse
	if err = json.Unmarshal(b, (*Alias)(v)); err != nil {
		return err
	}

	v.Metadata, err = view.ParseMetadataBytes(v.RawMetadata)

	return
}

// CreateView creates a new view in the catalog.
func (r *Catalog) CreateView(ctx context.Context, identifier table.Identifier, version *view.Version, schema *iceberg.Schema, opts ...catalog.CreateViewOpt) (*view.View, error) {
	if err := r.endpoints.check(endpointCreateView); err != nil {
		return nil, err
	}
	if version == nil {
		return nil, fmt.Errorf("%w: view version cannot be nil", iceberg.ErrInvalidArgument)
	}

	identPath, err := r.viewIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = []catalog.CreateViewOpt{}
	}
	cfg := catalog.NewCreateViewCfg()
	for _, opt := range opts {
		opt(&cfg)
	}

	freshSchema, err := iceberg.AssignFreshSchemaIDs(schema, nil)
	if err != nil {
		return nil, err
	}

	// Shallow copy for overriding certain attrs
	newVersion := *version
	version = &newVersion

	// Enforce starting ID of 1
	version.VersionID = 1

	// Set default catalog unless set by caller
	if len(version.DefaultCatalog) == 0 {
		version.DefaultCatalog = r.name
	}

	payload := createViewRequest{
		Name:        identPath.rawName,
		Location:    cfg.Location,
		Schema:      freshSchema,
		Props:       cfg.Properties,
		ViewVersion: version,
	}

	path, err := identPath.namespaceReqPath(endpointCreateView)
	if err != nil {
		return nil, err
	}

	ret, err := doPost[createViewRequest, viewResponse](ctx, r.baseURI, path, payload,
		r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace,
			http.StatusConflict: catalog.ErrViewAlreadyExists,
		})
	if err != nil {
		return nil, err
	}

	return view.New(identifier, ret.Metadata, ret.MetadataLoc, view.WithLabels(ret.Labels)), nil
}

// UpdateView updates a view in the catalog.
func (r *Catalog) UpdateView(ctx context.Context, ident table.Identifier, requirements []view.Requirement, updates []view.Update) (*view.View, error) {
	if err := r.endpoints.check(endpointUpdateView); err != nil {
		return nil, err
	}

	identPath, err := r.viewIdentifierPath(ident)
	if err != nil {
		return nil, err
	}

	restIdentifier := identifier{
		Namespace: catalog.NamespaceFromIdent(ident),
		Name:      identPath.rawName,
	}
	type payload struct {
		Identifier   identifier         `json:"identifier"`
		Requirements []view.Requirement `json:"requirements"`
		Updates      []view.Update      `json:"updates"`
	}
	path, err := identPath.objectReqPath(endpointUpdateView)
	if err != nil {
		return nil, err
	}

	ret, err := doPost[payload, viewResponse](ctx, r.baseURI, path,
		payload{Identifier: restIdentifier, Requirements: requirements, Updates: updates}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchView, http.StatusConflict: ErrCommitFailed})
	if err != nil {
		return nil, err
	}

	return view.New(ident, ret.Metadata, ret.MetadataLoc, view.WithLabels(ret.Labels)), nil
}

// loadViewResponse contains the response from loading a view
type loadViewResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	RawMetadata json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
	Labels      *iceberg.Labels    `json:"labels,omitempty"`
}

// RegisterView registers an existing view in the catalog using its metadata file location.
// The metadata file must already be accessible to the catalog. This is the view equivalent
// of RegisterTable, using the REST endpoint POST /namespaces/{ns}/register-view defined in
// the Iceberg REST catalog specification.
func (r *Catalog) RegisterView(ctx context.Context, identifier table.Identifier, metadataLoc string) (*view.View, error) {
	if err := r.endpoints.check(endpointRegisterView); err != nil {
		return nil, err
	}

	identPath, err := r.viewIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	type payload struct {
		Name        string `json:"name"`
		MetadataLoc string `json:"metadata-location"`
	}

	path, err := identPath.namespaceReqPath(endpointRegisterView)
	if err != nil {
		return nil, err
	}

	rsp, err := doPost[payload, loadViewResponse](ctx, r.baseURI, path,
		payload{Name: identPath.rawName, MetadataLoc: metadataLoc}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrViewAlreadyExists,
		})
	if err != nil {
		return nil, err
	}

	metadata, err := view.ParseMetadataBytes(rsp.RawMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse view metadata: %w", err)
	}

	return view.New(identifier, metadata, rsp.MetadataLoc, view.WithLabels(rsp.Labels)), nil
}

// LoadView loads a view from the catalog.
func (r *Catalog) LoadView(ctx context.Context, identifier table.Identifier) (*view.View, error) {
	if err := r.endpoints.check(endpointLoadView); err != nil {
		return nil, err
	}

	identPath, err := r.viewIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	path, err := identPath.objectReqPath(endpointLoadView)
	if err != nil {
		return nil, err
	}

	rsp, err := doGet[loadViewResponse](ctx, r.baseURI, path,
		r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchView,
		})
	if err != nil {
		return nil, err
	}

	metadata, err := view.ParseMetadataBytes(rsp.RawMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse view metadata: %w", err)
	}

	return view.New(identifier, metadata, rsp.MetadataLoc, view.WithLabels(rsp.Labels)), nil
}

func (r *Catalog) RenameView(ctx context.Context, from, to table.Identifier) (*view.View, error) {
	if err := r.endpoints.check(endpointRenameView); err != nil {
		return nil, err
	}
	if err := catalog.ValidateViewIdentifier(from); err != nil {
		return nil, err
	}
	if err := catalog.ValidateViewIdentifier(to); err != nil {
		return nil, err
	}

	type payload struct {
		Source      identifier `json:"source"`
		Destination identifier `json:"destination"`
	}
	src := identifier{
		Namespace: catalog.NamespaceFromIdent(from),
		Name:      catalog.ObjectNameFromIdent(from),
	}
	dst := identifier{
		Namespace: catalog.NamespaceFromIdent(to),
		Name:      catalog.ObjectNameFromIdent(to),
	}

	path, err := endpointRenameView.reqPath()
	if err != nil {
		return nil, err
	}

	_, err = doPost[payload, any](ctx, r.baseURI, path, payload{Source: src, Destination: dst}, r.cl,
		map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchView,
			http.StatusConflict: catalog.ErrViewAlreadyExists,
		}, allowNoContent())
	if err != nil {
		return nil, err
	}

	return r.LoadView(ctx, to)
}

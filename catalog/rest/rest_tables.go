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
	"fmt"
	"maps"
	"net/http"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

type commitTableResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	RawMetadata json.RawMessage `json:"metadata"`
}

// createTableRequest is the create-table wire format. This client only encodes
// it, from a spec and order already bound to the schema being sent. Source IDs
// a server decodes refer to the schema in the same request, under whatever
// numbering the client chose, so they need not be positive: decode into
// iceberg.UnboundPartitionSpec and table.UnboundSortOrder, then bind through
// table.NewMetadata (see #1664).
type createTableRequest struct {
	Name          string                 `json:"name"`
	Schema        *iceberg.Schema        `json:"schema"`
	Location      string                 `json:"location,omitempty"`
	PartitionSpec *iceberg.PartitionSpec `json:"partition-spec,omitempty"`
	WriteOrder    *table.SortOrder       `json:"write-order,omitempty"`
	StageCreate   bool                   `json:"stage-create"`
	Props         iceberg.Properties     `json:"properties,omitempty"`
}

func (r *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	if err := r.endpoints.check(endpointCreateTable); err != nil {
		return nil, err
	}

	identPath, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	// Resolve the reporter before the create POST mutates the server: it is
	// otherwise first built in the trailing tableFromResponse, so a bad
	// metrics-reporter-impl would turn a table the server already created into a
	// reported failure whose retry hits ErrTableAlreadyExists. CachedReporter
	// caches this for that tableFromResponse call. Resolved from the
	// client-supplied properties only (see the reporterProps field).
	if _, err := r.reporter.Get(r.reporterProps); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics reporter: %w", err)
	}

	cfg := catalog.NewCreateTableCfg()
	for _, o := range opts {
		o(&cfg)
	}

	if cfg.SortOrder.Fields() == nil && cfg.SortOrder.OrderID() == 0 {
		cfg.SortOrder = table.UnsortedSortOrder
	}

	stagedCreate := len(cfg.StagedUpdates) > 0

	payload := createTableRequest{
		Name:          identPath.rawName,
		Schema:        schema,
		Location:      cfg.Location,
		PartitionSpec: cfg.PartitionSpec,
		WriteOrder:    &cfg.SortOrder,
		StageCreate:   stagedCreate,
		Props:         cfg.Properties,
	}

	path, err := identPath.namespaceReqPath(endpointCreateTable)
	if err != nil {
		return nil, err
	}

	ret, err := doPost[createTableRequest, loadTableResponse](ctx, r.baseURI, path, payload,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrTableAlreadyExists})
	if err != nil {
		return nil, err
	}

	if stagedCreate {
		meta, metaLoc, err := r.commitStagedCreate(ctx, identifier, ret.Metadata, cfg.StagedUpdates)
		if err != nil {
			return nil, err
		}
		ret.Metadata = meta
		ret.MetadataLoc = metaLoc
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	// Save only the per-table configs.
	saved := maps.Clone(ret.Metadata.Properties())
	maps.Copy(config, ret.Config)
	maps.Copy(saved, ret.Config)
	scanPlanningConfig := maps.Clone(config)
	credsVended := len(ret.StorageCredentials) > 0
	vended := resolveStorageCredentials(ret.StorageCredentials, ret.MetadataLoc)
	mergeVendedCredentials(config, vended, ret.MetadataLoc)

	return r.tableFromResponse(ctx, identifier, ret.Metadata, ret.MetadataLoc, config, scanPlanningConfig, credsVended, vended, ret.Labels, table.WithSavedConfig(saved), table.WithScanPlanningDirective(r.scanPlanningDirective(ctx, identifier, ret.Config)))
}

// commitStagedCreate performs the second phase of a staged table
// creation by committing the table with an assert-create requirement.
// Changes are extracted from the phase 1 response metadata (which may
// have been normalized by the server), matching the Java implementation's
// createChanges approach. User-provided staged updates are appended
// after server-derived changes so that user intent takes precedence.
//
// If phase 2 fails, the server is left with an orphaned staged table
// (no automatic rollback).
func (r *Catalog) commitStagedCreate(ctx context.Context, identifier table.Identifier, meta table.Metadata, stagedUpdates []table.Update) (table.Metadata, string, error) {
	// Build updates from server-returned metadata, mirroring Java's
	// RESTSessionCatalog.createChanges.
	updates := make([]table.Update, 0, len(stagedUpdates)+10)
	updates = append(updates,
		table.NewAssignUUIDUpdate(meta.TableUUID()),
		table.NewUpgradeFormatVersionUpdate(meta.Version()),
		table.NewAddSchemaUpdate(meta.CurrentSchema()),
		table.NewSetCurrentSchemaUpdate(-1),
	)

	spec := meta.PartitionSpec()
	if !spec.IsUnpartitioned() {
		updates = append(updates, table.NewAddPartitionSpecUpdate(&spec, true))
	} else {
		updates = append(updates, table.NewAddPartitionSpecUpdate(iceberg.UnpartitionedSpec, true))
	}
	updates = append(updates, table.NewSetDefaultSpecUpdate(-1))

	order := meta.SortOrder()
	if !order.IsUnsorted() {
		updates = append(updates, table.NewAddSortOrderUpdate(&order))
	} else {
		updates = append(updates, table.NewAddSortOrderUpdate(&table.UnsortedSortOrder))
	}
	updates = append(updates, table.NewSetDefaultSortOrderUpdate(-1))

	if loc := meta.Location(); loc != "" {
		updates = append(updates, table.NewSetLocationUpdate(loc))
	}
	if props := meta.Properties(); len(props) > 0 {
		updates = append(updates, table.NewSetPropertiesUpdate(props))
	}

	// Append user-provided staged updates at the end.
	updates = append(updates, stagedUpdates...)

	return r.CommitTable(ctx, identifier, table.RequirementsForCreateTable(updates), updates)
}

func (r *Catalog) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	if err := r.endpoints.check(endpointUpdateTable); err != nil {
		return nil, "", err
	}

	identPath, err := r.tableIdentifierPath(ident)
	if err != nil {
		return nil, "", err
	}

	restIdentifier := identifier{
		Namespace: catalog.NamespaceFromIdent(ident),
		Name:      identPath.rawName,
	}

	type payload struct {
		Identifier   identifier          `json:"identifier"`
		Requirements []table.Requirement `json:"requirements"`
		Updates      []table.Update      `json:"updates"`
	}

	path, err := identPath.objectReqPath(endpointUpdateTable)
	if err != nil {
		return nil, "", err
	}

	ret, err := doPost[payload, commitTableResponse](ctx, r.baseURI, path,
		payload{Identifier: restIdentifier, Requirements: requirements, Updates: updates}, r.cl,
		map[int]error{
			http.StatusNotFound:            catalog.ErrNoSuchTable,
			http.StatusConflict:            ErrCommitFailed,
			http.StatusInternalServerError: ErrCommitStateUnknown,
			http.StatusBadGateway:          ErrCommitStateUnknown,
			http.StatusServiceUnavailable:  ErrCommitStateUnknown,
			http.StatusGatewayTimeout:      ErrCommitStateUnknown,
		})
	if err != nil {
		return nil, "", err
	}
	metadata, err := r.parseCommitMetadata(ret.RawMetadata)
	if err != nil {
		return nil, "", err
	}

	return metadata, ret.MetadataLoc, nil
}

func (r *Catalog) parseCommitMetadata(raw json.RawMessage) (table.Metadata, error) {
	if r.snapshotMode == SnapshotModeRefs {
		return table.ParseMetadataBytesDeferredSnapshots(raw)
	}

	return table.ParseMetadataBytes(raw)
}

// CommitTransaction atomically commits changes to multiple tables in a
// single request. It implements [catalog.TransactionalCatalog].
//
// The server applies all changes or none (all-or-nothing). On success
// (204 No Content) the method returns nil. Callers must LoadTable
// individually to obtain updated metadata.
func (r *Catalog) CommitTransaction(ctx context.Context, commits []table.TableCommit) error {
	if len(commits) == 0 {
		return catalog.ErrEmptyCommitList
	}

	if err := r.endpoints.check(endpointCommitTransaction); err != nil {
		return err
	}

	type tableChange struct {
		Identifier   identifier          `json:"identifier"`
		Requirements []table.Requirement `json:"requirements"`
		Updates      []table.Update      `json:"updates"`
	}

	type payload struct {
		TableChanges []tableChange `json:"table-changes"`
	}

	changes := make([]tableChange, len(commits))
	for i, c := range commits {
		if len(c.Identifier) == 0 {
			return catalog.ErrMissingIdentifier
		}
		if err := catalog.ValidateTableIdentifier(c.Identifier); err != nil {
			return err
		}

		reqs := c.Requirements
		if reqs == nil {
			reqs = []table.Requirement{}
		}
		updates := c.Updates
		if updates == nil {
			updates = []table.Update{}
		}

		changes[i] = tableChange{
			Identifier: identifier{
				Namespace: catalog.NamespaceFromIdent(c.Identifier),
				Name:      catalog.ObjectNameFromIdent(c.Identifier),
			},
			Requirements: reqs,
			Updates:      updates,
		}
	}

	path, err := endpointCommitTransaction.reqPath()
	if err != nil {
		return err
	}

	_, err = doPost[payload, struct{}](
		ctx, r.baseURI, path,
		payload{TableChanges: changes}, r.cl,
		map[int]error{
			http.StatusNotFound:            catalog.ErrNoSuchTable,
			http.StatusConflict:            ErrCommitFailed,
			http.StatusInternalServerError: ErrCommitStateUnknown,
			http.StatusBadGateway:          ErrCommitStateUnknown,
			http.StatusServiceUnavailable:  ErrCommitStateUnknown,
			http.StatusGatewayTimeout:      ErrCommitStateUnknown,
		},
		allowNoContent(),
	)

	return err
}

func (r *Catalog) RegisterTable(ctx context.Context, identifier table.Identifier, metadataLoc string) (*table.Table, error) {
	if err := r.endpoints.check(endpointRegisterTable); err != nil {
		return nil, err
	}

	identPath, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	type payload struct {
		Name        string `json:"name"`
		MetadataLoc string `json:"metadata-location"`
	}

	// Resolve the reporter before the register POST mutates the server, for the
	// same reason as CreateTable: it is otherwise first built in the trailing
	// tableFromResponse, so an invalid reporter would turn a successful
	// registration into a reported failure. CachedReporter caches this. Resolved
	// from the client-supplied properties only (see the reporterProps field).
	if _, err := r.reporter.Get(r.reporterProps); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics reporter: %w", err)
	}

	path, err := identPath.namespaceReqPath(endpointRegisterTable)
	if err != nil {
		return nil, err
	}

	ret, err := doPost[payload, loadTableResponse](ctx, r.baseURI, path,
		payload{Name: identPath.rawName, MetadataLoc: metadataLoc}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrTableAlreadyExists,
		})
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	saved := maps.Clone(ret.Metadata.Properties())
	maps.Copy(config, ret.Config)
	maps.Copy(saved, ret.Config)
	scanPlanningConfig := maps.Clone(config)
	credsVended := len(ret.StorageCredentials) > 0
	vended := resolveStorageCredentials(ret.StorageCredentials, ret.MetadataLoc)
	mergeVendedCredentials(config, vended, ret.MetadataLoc)

	return r.tableFromResponse(ctx, identifier, ret.Metadata, ret.MetadataLoc, config, scanPlanningConfig, credsVended, vended, ret.Labels, table.WithSavedConfig(saved), table.WithScanPlanningDirective(r.scanPlanningDirective(ctx, identifier, ret.Config)))
}

func (r *Catalog) UpdateTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (*table.Table, error) {
	if err := r.endpoints.check(endpointUpdateTable); err != nil {
		return nil, err
	}

	identPath, err := r.tableIdentifierPath(ident)
	if err != nil {
		return nil, err
	}

	restIdentifier := identifier{
		Namespace: catalog.NamespaceFromIdent(ident),
		Name:      identPath.rawName,
	}
	type payload struct {
		Identifier   identifier          `json:"identifier"`
		Requirements []table.Requirement `json:"requirements"`
		Updates      []table.Update      `json:"updates"`
	}
	path, err := identPath.objectReqPath(endpointUpdateTable)
	if err != nil {
		return nil, err
	}

	ret, err := doPost[payload, commitTableResponse](ctx, r.baseURI, path,
		payload{Identifier: restIdentifier, Requirements: requirements, Updates: updates}, r.cl,
		map[int]error{
			http.StatusNotFound:            catalog.ErrNoSuchTable,
			http.StatusConflict:            ErrCommitFailed,
			http.StatusInternalServerError: ErrCommitStateUnknown,
			http.StatusBadGateway:          ErrCommitStateUnknown,
			http.StatusServiceUnavailable:  ErrCommitStateUnknown,
			http.StatusGatewayTimeout:      ErrCommitStateUnknown,
		})
	if err != nil {
		return nil, err
	}
	metadata, err := r.parseCommitMetadata(ret.RawMetadata)
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, metadata.Properties())

	// A commit response carries no labels or table config (they are load-time
	// enrichment). The catalog's own scan-planning-mode property still applies,
	// so the returned table enforces a catalog-wide directive; a per-table
	// directive from the server's load config needs a reload.
	return r.tableFromResponse(ctx, ident, metadata, ret.MetadataLoc, config, config, false, nil, nil,
		table.WithSavedConfig(metadata.Properties()),
		table.WithScanPlanningDirective(r.scanPlanningDirective(ctx, ident, nil)))
}

func (r *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	if err := r.endpoints.check(endpointDeleteTable); err != nil {
		return err
	}

	identPath, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return err
	}

	path, err := identPath.objectReqPath(endpointDeleteTable)
	if err != nil {
		return err
	}

	uri := r.baseURI.JoinPath(path...)
	v := url.Values{}
	v.Set("purgeRequested", "false")
	uri.RawQuery = v.Encode()

	_, err = doDelete[struct{}](ctx, uri, []string{}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	r.tables.invalidate(identifier)

	return err
}

func (r *Catalog) PurgeTable(ctx context.Context, identifier table.Identifier) error {
	if err := r.endpoints.check(endpointDeleteTable); err != nil {
		return err
	}

	identPath, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return err
	}

	path, err := identPath.objectReqPath(endpointDeleteTable)
	if err != nil {
		return err
	}

	uri := r.baseURI.JoinPath(path...)
	v := url.Values{}
	v.Set("purgeRequested", "true")
	uri.RawQuery = v.Encode()

	_, err = doDelete[struct{}](ctx, uri, []string{}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	r.tables.invalidate(identifier)

	return err
}

func (r *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	if err := r.endpoints.check(endpointRenameTable); err != nil {
		return nil, err
	}
	if err := catalog.ValidateTableIdentifier(from); err != nil {
		return nil, err
	}
	if err := catalog.ValidateTableIdentifier(to); err != nil {
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

	path, err := endpointRenameTable.reqPath()
	if err != nil {
		return nil, err
	}

	// The spec's rename 404 covers both a missing source table and a missing
	// destination namespace; split it by error.type so a missing namespace is
	// not reported as a missing table. The type override only refines a
	// status mapped here, so a 5xx or 401/403 carrying that type is unaffected.
	_, err = doPost[payload, any](ctx, r.baseURI, path, payload{Source: src, Destination: dst}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable, http.StatusConflict: catalog.ErrTableAlreadyExists},
		allowNoContent(), withErrorTypeOverride(map[string]error{errTypeNoSuchNamespace: catalog.ErrNoSuchNamespace}))
	if err != nil {
		return nil, err
	}
	r.tables.invalidate(from)

	return r.LoadTable(ctx, to)
}

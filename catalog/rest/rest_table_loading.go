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
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/semaphore"
)

// StorageCredential carries REST-vended storage credentials scoped to matching
// object-location prefixes.
type StorageCredential struct {
	Prefix string             `json:"prefix"`
	Config iceberg.Properties `json:"config"`
}

type loadTableResponse struct {
	MetadataLoc        string              `json:"metadata-location"`
	RawMetadata        json.RawMessage     `json:"metadata"`
	Config             iceberg.Properties  `json:"config"`
	StorageCredentials []StorageCredential `json:"storage-credentials"`
	Labels             *iceberg.Labels     `json:"labels,omitempty"`
	Metadata           table.Metadata      `json:"-"`
}

type loadCredentialsResponse struct {
	StorageCredentials []StorageCredential `json:"storage-credentials"`
}

func (t *loadTableResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias loadTableResponse
	if err = json.Unmarshal(b, (*Alias)(t)); err != nil {
		return err
	}

	t.Metadata, err = table.ParseMetadataBytes(t.RawMetadata)

	return err
}

func (r *Catalog) tableFromResponse(
	_ context.Context,
	identifier []string,
	metadata table.Metadata,
	loc string,
	config iceberg.Properties,
	scanPlanningConfig iceberg.Properties,
	credsVended bool,
	vended iceberg.Properties,
	labels *iceberg.Labels,
	opts ...table.Option,
) (*table.Table, error) {
	var fsF func(context.Context) (iceio.IO, error)
	if credsVended {
		refresher := &vendedCredentialRefresher{
			mu:         semaphore.NewWeighted(1),
			identifier: identifier,
			location:   loc,
			props:      config,
			vended:     vended,
			fetchCreds: func(ctx context.Context, ident []string) (iceberg.Properties, error) {
				return r.fetchTableCreds(ctx, ident, loc)
			},
		}
		fsF = refresher.loadFS
	} else {
		fsF = iceio.LoadFSFunc(config, loc)
	}

	// Resolve the reporter from the client-supplied properties only (see the
	// reporterProps field). r.props folds in server-vended /v1/config and, per
	// call, table metadata and creds — any of which could otherwise shadow the
	// client's reporter choice or turn an unknown server-side name into a hard
	// load error. Server-vended reporter selection, if ever wanted, should be an
	// explicit decision rather than a side effect of merge order.
	reporter, err := r.reporter.Get(r.reporterProps)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics reporter: %w", err)
	}
	// Opt-in: POST scan/commit reports to the catalog's metrics endpoint via the
	// catalog-owned dispatcher. It is non-nil only when the client enabled
	// reporting and the server advertises the endpoint (see init).
	if r.metricsDispatcher != nil {
		if identPath, idErr := r.tableIdentifierPath(identifier); idErr != nil {
			// Unreachable for a valid identifier that already loaded above, but if it
			// ever regresses, log why the REST reporter was dropped rather than let
			// metrics silently vanish with no signal.
			slog.Debug("iceberg: skipping REST metrics reporter, cannot build identifier path",
				"error", idErr)
		} else if path, pErr := identPath.objectReqPath(endpointReportMetrics); pErr != nil {
			slog.Debug("iceberg: skipping REST metrics reporter, cannot build metrics path",
				"error", pErr)
		} else {
			reporter = metrics.Combine(reporter, &restMetricsReporter{
				baseURI:    r.baseURI,
				cl:         r.cl,
				path:       path,
				dispatcher: r.metricsDispatcher,
			})
		}
	}

	// Caller-supplied opts are applied last so they can override the defaults
	// derived above.
	return table.New(
		identifier,
		metadata,
		loc,
		fsF,
		r,
		append([]table.Option{
			table.WithMetricsReporter(reporter),
			table.WithScanPlanningIOProperties(scanPlanningConfig),
			table.WithLabels(labels),
		}, opts...)...,
	), nil
}

// scanPlanningDirective resolves a table's raw scan-planning-mode directive and
// whether one is present, in the shape table.WithScanPlanningDirective takes.
// As in Java's RESTSessionCatalog, the server's table config takes precedence
// and the catalog's own scan-planning-mode property applies when the server
// sends none; table metadata is never consulted. r.props already folds in the
// /v1/config defaults and overrides. A mismatch between the two is logged as a
// warning once per catalog and at debug level after that, since Refresh
// reloads the table. A nil loadConfig yields the catalog property alone.
func (r *Catalog) scanPlanningDirective(ctx context.Context, identifier []string, loadConfig iceberg.Properties) (string, bool) {
	server, hasServer := loadConfig[table.ScanPlanningModeKey]
	client, hasClient := r.props[table.ScanPlanningModeKey]
	if !hasServer {
		return client, hasClient
	}

	if hasClient && !strings.EqualFold(client, server) {
		level := slog.LevelDebug
		if r.scanPlanningMismatchWarned.CompareAndSwap(false, true) {
			level = slog.LevelWarn
		}
		slog.Log(ctx, level, "iceberg: scan-planning-mode mismatch between catalog and server; using the server value",
			"table", strings.Join(identifier, "."), "client", client, "server", server)
	}

	return server, true
}

func (r *Catalog) fetchTableCreds(ctx context.Context, ident []string, location string) (iceberg.Properties, error) {
	if err := r.endpoints.check(endpointTableCredentials); err != nil {
		return nil, err
	}

	identPath, err := r.tableIdentifierPath(ident)
	if err != nil {
		return nil, err
	}

	path, err := identPath.objectReqPath(endpointTableCredentials)
	if err != nil {
		return nil, err
	}

	ret, err := doGet[loadCredentialsResponse](ctx, r.baseURI, path,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	if err != nil {
		return nil, err
	}

	return resolveStorageCredentials(ret.StorageCredentials, location), nil
}

// RefreshTableCredentials updates a *table.Table with newly-vended credentials from the catalog
// without updating any other table-internal state that a full Refresh() would.
// Allows for a quick table credential refresh if the table was created without any pre-seeded
// credentials. If the catalog did not vend any credentials, the table is returned unmodified.
//
// Note that the passed-in table instance should be created with the table.WithSavedConfig() option to
// save any table-specific configs for reuse in the newly-created instance. All tables created by this
// catalog pass in that option. Callers of table.New() that use this method should also pass
// metadata.Properties() into table.WithSavedConfig().
func (r *Catalog) RefreshTableCredentials(ctx context.Context, tbl *table.Table) (*table.Table, error) {
	metadataLoc := tbl.MetadataLocation()
	resp, err := r.fetchTableCreds(ctx, tbl.Identifier(), metadataLoc)
	if err != nil {
		return nil, err
	}
	if len(resp) == 0 {
		// No new credentials vended. Return as-is.
		return tbl, nil
	}

	// Return a new *table.Table with newly-merged credentials coming from the
	// fetchTableCreds call.
	config := maps.Clone(r.props)
	maps.Copy(config, tbl.SavedConfig())
	scanCfg := maps.Clone(config)
	mergeVendedCredentials(config, resp, metadataLoc)

	opts := []table.Option{
		table.WithSavedConfig(tbl.SavedConfig()),
		// Credential refresh does not return table config, so carry the
		// existing scan-planning directive over verbatim.
		table.WithScanPlanningDirectiveFrom(tbl),
	}
	// Keep a reporter the caller set on the table rather than reverting it to
	// the catalog default, as Refresh does.
	if reporter := tbl.MetricsReporter(); !metrics.IsNop(reporter) {
		opts = append(opts, table.WithMetricsReporter(reporter))
	}

	return r.tableFromResponse(ctx, tbl.Identifier(), tbl.Metadata(), metadataLoc, config, scanCfg, true, resp, tbl.Labels(), opts...)
}

// ListTables lists the tables in a namespace. If the server does not advertise
// the list-tables endpoint, it yields no tables rather than an error.
func (r *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		pageSize := r.getPageSize(ctx)
		err := paginateIdentifiers(func(pageToken string) ([]table.Identifier, string, error) {
			return r.listTablesPage(ctx, namespace, pageToken, pageSize)
		}, func(identifier table.Identifier) bool {
			return yield(identifier, nil)
		})
		if err != nil {
			yield(table.Identifier{}, err)
		}
	}
}

func (r *Catalog) listTablesPage(ctx context.Context, namespace table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	return listCatalogObjectsPage(r, ctx, endpointListTables, namespace, pageToken, pageSize, objectIdentifier)
}

// LoadTable loads a table from the catalog. It implements [catalog.Catalog].
// When snapshot-loading-mode is set to "refs" in the catalog properties, only
// snapshots referenced by a named branch or tag are included in the response.
// Callers that need to time-travel to an unreferenced snapshot should use
// snapshot-loading-mode "all" or omit the property entirely.
func (r *Catalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	return r.loadTableWithMode(ctx, identifier, r.snapshotMode)
}

func (r *Catalog) loadTableWithMode(ctx context.Context, identifier table.Identifier, mode SnapshotMode) (*table.Table, error) {
	if err := r.endpoints.check(endpointLoadTable); err != nil {
		return nil, err
	}

	identPath, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return nil, err
	}

	path, err := identPath.objectReqPath(endpointLoadTable)
	if err != nil {
		return nil, err
	}

	var etag string
	opts := []reqOption{withResponseHeaders(func(h http.Header) { etag = h.Get("ETag") })}
	if mode != "" {
		opts = append(opts, withQueryParams(url.Values{"snapshots": {string(mode)}}))
	}
	cached, hit := r.tables.get(identifier, mode)
	if hit {
		opts = append(opts, withIfNoneMatch(cached.etag))
	}

	ret, err := doGet[loadTableResponse](ctx, r.baseURI, path,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable}, opts...)
	switch {
	case errors.Is(err, errNotModified):
		ret = cached.rsp
	case errors.Is(err, catalog.ErrNoSuchTable):
		r.tables.invalidate(identifier)

		return nil, err
	case err != nil:
		return nil, err
	case etag != "":
		r.tables.put(identifier, mode, etag, ret)
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	// Save only the per-table configs, not r.props.
	saved := maps.Clone(ret.Metadata.Properties())
	maps.Copy(config, ret.Config)
	maps.Copy(saved, ret.Config)
	scanPlanningConfig := maps.Clone(config)
	credsVended := len(ret.StorageCredentials) > 0
	vended := resolveStorageCredentials(ret.StorageCredentials, ret.MetadataLoc)
	mergeVendedCredentials(config, vended, ret.MetadataLoc)

	return r.tableFromResponse(ctx, identifier, ret.Metadata, ret.MetadataLoc, config, scanPlanningConfig, credsVended, vended, ret.Labels, table.WithSavedConfig(saved), table.WithScanPlanningDirective(r.scanPlanningDirective(ctx, identifier, ret.Config)))
}

func (r *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	identPath, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return false, err
	}

	// No HEAD endpoint: fall back to GET (load). If load is unsupported too, its
	// ErrEndpointNotSupported surfaces rather than a bogus "not found".
	if !r.endpoints.contains(endpointTableExists) {
		_, err := r.LoadTable(ctx, identifier)
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchTable) {
				return false, nil
			}

			return false, err
		}

		return true, nil
	}

	path, err := identPath.objectReqPath(endpointTableExists)
	if err != nil {
		return false, err
	}

	err = doHead(ctx, r.baseURI, path,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			r.tables.invalidate(identifier)

			return false, nil
		}

		return false, err
	}

	return true, nil
}

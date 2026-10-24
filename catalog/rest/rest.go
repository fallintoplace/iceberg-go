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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"golang.org/x/sync/semaphore"
)

var (
	_ catalog.Catalog                      = (*Catalog)(nil)
	_ catalog.TransactionalCatalog         = (*Catalog)(nil)
	_ catalog.RefreshableCredentialCatalog = (*Catalog)(nil)
)

const (
	pageSizeKey contextKey = "page_size"
	skipOAuth   contextKey = "skip_oauth"

	defaultPageSize = 20

	keyOauthToken        = "token"
	keyWarehouseLocation = "warehouse"
	keyMetadataLocation  = "metadata_location"
	keyOauthCredential   = "credential"
	keyScope             = "scope"
	keyAudience          = "audience"
	keyResource          = "resource"

	authorizationHeader = "Authorization"
	bearerPrefix        = "Bearer"
	keyPrefix           = "prefix"

	// headerIcebergAccessDelegation requests server-side storage-credential
	// vending. It is a session default (defaultAccessDelegation) and a
	// per-request header on the scan-planning endpoints that vend credentials.
	headerIcebergAccessDelegation = "X-Iceberg-Access-Delegation"
	defaultAccessDelegation       = "vended-credentials"

	keyNamespaceSeparator     = "namespace-separator"
	defaultNamespaceSeparator = "%1F"

	icebergRestSpecVersion = "0.14.1"

	keyRestSigV4        = "rest.sigv4-enabled"
	keyRestSigV4Region  = "rest.signing-region"
	keyRestSigV4Service = "rest.signing-name"
	keyAuthUrl          = "rest.authorization-url"
	// keyOAuth2ServerURI is the portable, spec-aligned property for the OAuth2
	// token endpoint used by Java, PyIceberg and iceberg-rust. It is the
	// preferred key; keyAuthUrl is retained as a compatibility alias. When both
	// are set, oauth2-server-uri takes precedence.
	keyOAuth2ServerURI = "oauth2-server-uri"
	keyTlsSkipVerify   = "rest.tls.skip-verify"

	keyViewEndpointsSupported = "view-endpoints-supported"
	keySnapshotLoadingMode    = "snapshot-loading-mode"
)

var (
	ErrRESTError            = errors.New("REST error")
	ErrBadRequest           = fmt.Errorf("%w: bad request", ErrRESTError)
	ErrForbidden            = fmt.Errorf("%w: forbidden", ErrRESTError)
	ErrUnauthorized         = fmt.Errorf("%w: unauthorized", ErrRESTError)
	ErrAuthorizationExpired = fmt.Errorf("%w: authorization expired", ErrRESTError)
	ErrServiceUnavailable   = fmt.Errorf("%w: service unavailable", ErrRESTError)
	ErrServerError          = fmt.Errorf("%w: server error", ErrRESTError)
	// ErrCommitFailed wraps both ErrRESTError and table.ErrCommitFailed
	// so that callers can detect retryable commit conflicts via
	// errors.Is(err, table.ErrCommitFailed).
	ErrCommitFailed       = fmt.Errorf("%w: %w", ErrRESTError, table.ErrCommitFailed)
	ErrCommitStateUnknown = fmt.Errorf("%w: commit failed due to unknown reason", ErrRESTError)
	ErrOAuthError         = fmt.Errorf("%w: oauth error", ErrRESTError)

	// errNotModified reports a 304 to a request sent with withIfNoneMatch.
	errNotModified = errors.New("not modified")
)

// SnapshotMode controls which snapshots are included in a loadTable response.
type SnapshotMode string

const (
	// SnapshotModeAll requests all currently valid snapshots (server default).
	SnapshotModeAll SnapshotMode = "all"
	// SnapshotModeRefs requests only snapshots that are referenced by at least one named branch or tag.
	// The server omits unreferenced historical snapshots, which reduces response size for tables
	// with long snapshot histories. Note: time-travel to a snapshot not referenced by any branch
	// or tag will fail, as that snapshot will not be present in the returned metadata.
	SnapshotModeRefs SnapshotMode = "refs"
)

func init() {
	reg := catalog.RegistrarFunc(func(ctx context.Context, name string, p iceberg.Properties) (catalog.Catalog, error) {
		return newCatalogFromProps(ctx, name, p.Get("uri", ""), p)
	})

	catalog.Register(string(catalog.REST), reg)
	catalog.Register("http", reg)
	catalog.Register("https", reg)
}

type errorResponse struct {
	Message string   `json:"message"`
	Type    string   `json:"type"`
	Code    int      `json:"code"`
	Stack   []string `json:"stack,omitempty"`

	wrapping   error
	statusCode int
	retryAfter string
}

type contextKey string

func (e errorResponse) Unwrap() error { return e.wrapping }
func (e errorResponse) Error() string {
	switch {
	case e.Type != "" && e.Message != "":
		return e.Type + ": " + e.Message
	case e.Message != "":
		return e.Message
	case e.Type != "":
		return e.Type
	case e.wrapping != nil:
		return e.wrapping.Error()
	default:
		return "unknown REST error"
	}
}

type identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type commitTableResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	RawMetadata json.RawMessage `json:"metadata"`
}

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

type configResponse struct {
	Defaults  iceberg.Properties `json:"defaults"`
	Overrides iceberg.Properties `json:"overrides"`
	Endpoints []string           `json:"endpoints,omitempty"`
}

func fromProps(props iceberg.Properties, o *options) error {
	// The OAuth token endpoint may be configured via either the portable
	// oauth2-server-uri key or the legacy rest.authorization-url alias. Capture
	// both and resolve precedence after the loop, since map iteration order is
	// non-deterministic.
	var authURL, oauth2ServerURI *url.URL
	for k, v := range props {
		switch k {
		case keyOauthToken:
			if o.oauthToken == "" {
				o.oauthToken = v
			}
		case keyWarehouseLocation:
			o.warehouseLocation = v
		case keyMetadataLocation:
			o.metadataLocation = v
		case keyRestSigV4:
			o.enableSigv4 = strings.ToLower(v) == "true"
		case keyRestSigV4Region:
			o.sigv4Region = v
		case keyRestSigV4Service:
			o.sigv4Service = v
		case keyAuthUrl:
			u, err := parseAuthURL(keyAuthUrl, v)
			if err != nil {
				return err
			}
			authURL = u
		case keyOAuth2ServerURI:
			u, err := parseAuthURL(keyOAuth2ServerURI, v)
			if err != nil {
				return err
			}
			oauth2ServerURI = u
		case keyOauthCredential:
			o.credential = v
		case keyScope:
			o.scope = v
		case keyAudience:
			o.audience = v
		case keyResource:
			o.resource = v
		case keyPrefix:
			o.prefix = v
		case keyTlsSkipVerify:
			verify := strings.ToLower(v) == "true"
			if o.tlsConfig == nil {
				o.tlsConfig = &tls.Config{
					InsecureSkipVerify: verify,
				}
			} else {
				o.tlsConfig.InsecureSkipVerify = verify
			}
		case "uri", "type":
		default:
			if v != "" {
				if o.additionalProps == nil {
					o.additionalProps = iceberg.Properties{}
				}
				o.additionalProps[k] = v
			}
		}
	}

	// oauth2-server-uri is the portable, preferred key and takes precedence over
	// the rest.authorization-url alias when both are set.
	switch {
	case oauth2ServerURI != nil:
		o.authUri = oauth2ServerURI
	case authURL != nil:
		o.authUri = authURL
	}

	return nil
}

// resolveAuthURLAlias collapses the oauth2-server-uri / rest.authorization-url
// alias within a single configuration layer into the canonical
// oauth2-server-uri key. oauth2-server-uri wins over the rest.authorization-url
// alias within the same layer. Collapsing per layer before merging keeps
// defaults -> client -> overrides precedence intact.
func resolveAuthURLAlias(props iceberg.Properties) {
	v, ok := props[keyOAuth2ServerURI]
	if !ok {
		v, ok = props[keyAuthUrl]
	}
	delete(props, keyAuthUrl)
	if ok {
		props[keyOAuth2ServerURI] = v
	}
}

func parseAuthURL(key, raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid %s %q: %w", key, raw, err)
	}
	if u.Scheme == "" {
		return nil, fmt.Errorf("invalid %s %q: missing scheme", key, raw)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid %s %q: missing host", key, raw)
	}

	return u, nil
}

func toProps(o *options) iceberg.Properties {
	props := iceberg.Properties{}
	maps.Copy(props, o.additionalProps)

	setIf := func(key, v string) {
		if v != "" {
			props[key] = v
		}
	}

	setIf(keyOauthToken, o.oauthToken)
	setIf(keyOauthCredential, o.credential)
	setIf(keyWarehouseLocation, o.warehouseLocation)
	setIf(keyMetadataLocation, o.metadataLocation)
	if o.enableSigv4 {
		props[keyRestSigV4] = "true"
		setIf(keyRestSigV4Region, o.sigv4Region)
		setIf(keyRestSigV4Service, o.sigv4Service)

		// Best-effort fallback: propagate the SigV4 signing region as
		// a client region hint so that S3 I/O can determine the correct
		// regional endpoint when s3.region is not explicitly set.
		// Only applied for S3/S3Tables services where the signing region
		// is likely to match the bucket region.
		if o.sigv4Region != "" && (o.sigv4Service == "s3" || o.sigv4Service == "s3tables") {
			if _, ok := props[iceio.S3ClientRegion]; !ok {
				props[iceio.S3ClientRegion] = o.sigv4Region
			}
		}
	}

	setIf(keyPrefix, o.prefix)
	if o.authUri != nil {
		// Advertise the endpoint only under the portable oauth2-server-uri key.
		// We canonicalize to this key everywhere else (fromProps precedence,
		// resolveAuthURLAlias), so emitting the legacy rest.authorization-url
		// alias too would leave the endpoint under two names in r.props and any
		// table config cloned from it - two sources of truth that can drift.
		setIf(keyOAuth2ServerURI, o.authUri.String())
	}

	return props
}

var _ catalog.PurgeableTable = (*Catalog)(nil)

type Catalog struct {
	baseURI *url.URL
	cl      *http.Client
	// closeSession releases transports owned by the catalog. Caller-provided
	// transports are excluded when the session is constructed.
	closeSession     func()
	closeSessionOnce sync.Once
	closeErr         error

	name string
	// Retained catalog properties are reused for table/view IO and may carry
	// authentication material such as credential or token.
	props     iceberg.Properties
	endpoints endpointSet

	namespaceSeparator string
	snapshotMode       SnapshotMode
	// tables holds ETag-tagged load responses for freshness-aware loading; nil
	// when rest-table-cache.max-entries is 0.
	tables *tableCache

	// reporter builds and caches the catalog's metrics reporter once, so it is
	// constructed per-catalog rather than per table load. Released by Close.
	reporter metrics.CachedReporter
	// reporterProps holds only the client-supplied properties, captured before
	// the server's /v1/config defaults and overrides are merged into props. The
	// metrics reporter is resolved from these so a server-vended
	// metrics-reporter-impl cannot select (or, via an unregistered name, break)
	// a reporter the client never asked for.
	reporterProps iceberg.Properties
	// metricsDispatcher POSTs scan/commit reports to the server's metrics
	// endpoint on a bounded worker pool shared across this catalog's table
	// reporters. It is nil unless the client opted in (reportMetricsEnabled, read
	// from reporterProps) and the server advertises the endpoint. Owned here and
	// closed by Close.
	metricsDispatcher *metricsDispatcher
	// scanPlanningMismatchWarned limits the scan-planning-mode mismatch warning
	// to once per catalog; see scanPlanningDirective.
	scanPlanningMismatchWarned atomic.Bool
}

func newCatalogFromProps(ctx context.Context, name string, uri string, p iceberg.Properties) (*Catalog, error) {
	var ops options
	if err := fromProps(p, &ops); err != nil {
		return nil, err
	}

	r := &Catalog{name: name}
	if err := r.init(ctx, &ops, uri); err != nil {
		return nil, err
	}

	return r, nil
}

func NewCatalog(ctx context.Context, name, uri string, opts ...Option) (*Catalog, error) {
	ops := &options{}
	for _, o := range opts {
		o(ops)
	}

	if ops.tlsConfig != nil && ops.transport != nil {
		return nil, errors.New("invalid catalog config with non-nil tlsConfig and transport: tlsConfig will be ignored, it should be added to the provided transport instead")
	}

	r := &Catalog{name: name}
	if err := r.init(ctx, ops, uri); err != nil {
		return nil, err
	}

	return r, nil
}

// defaultOAuthTimeout bounds a single OAuth token-refresh request made by the
// built-in Oauth2AuthManager. oauth2's TokenSource.Token() takes no context, so
// this Timeout on the refresh client is what keeps a stalled or black-holing
// token endpoint from blocking auth — and therefore any request that triggers a
// refresh, including asynchronous metrics reports — indefinitely.
const defaultOAuthTimeout = 60 * time.Second

// setupOAuthManager creates an Oauth2AuthManager based on the provided options.
// It uses golang.org/x/oauth2 for token management, caching, and thread-safe refresh.
// The returned cleanup closes an internally-created OAuth transport, if any.
func setupOAuthManager(r *Catalog, cl *http.Client, opts *options) (AuthManager, func()) {
	// If a static token is provided, use it directly.
	if opts.oauthToken != "" {
		return &Oauth2AuthManager{
			tokenSource: oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: opts.oauthToken,
				TokenType:   "Bearer",
			}),
		}, nil
	}

	// If no credential, no auth needed.
	if opts.credential == "" {
		return nil, nil
	}

	authURL := opts.authUri
	if authURL == nil {
		authURL = r.baseURI.JoinPath("oauth/tokens")
	}

	clientID, clientSecret, found := strings.Cut(opts.credential, ":")
	if !found {
		clientID = ""
		clientSecret = opts.credential
	}

	cfg := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     authURL.String(),
		AuthStyle:    oauth2.AuthStyleInParams,
	}

	scope := "catalog"
	if opts.scope != "" {
		scope = opts.scope
	}
	cfg.Scopes = []string{scope}

	if opts.audience != "" || opts.resource != "" {
		cfg.EndpointParams = url.Values{}
		if opts.audience != "" {
			cfg.EndpointParams.Set("audience", opts.audience)
		}
		if opts.resource != "" {
			cfg.EndpointParams.Set("resource", opts.resource)
		}
	}

	// Add skip oauth so we don't get in cycles trying to refresh the token
	ctx := context.WithValue(context.Background(), skipOAuth, true)

	// Token refresh uses a client with a Timeout so a stalled token endpoint
	// cannot block auth forever (oauth2's Token() takes no context). By default
	// this reuses the catalog client's transport — preserving its TLS, proxy and
	// header behavior — but as a distinct *http.Client so the Timeout applies to
	// refresh alone and not to ordinary catalog requests.
	oauthClient := &http.Client{
		Transport:     cl.Transport,
		Timeout:       defaultOAuthTimeout,
		CheckRedirect: sameOriginRedirectsOnly,
	}
	var closeIdleConnections func()
	if opts.oauthTLSConfig != nil {
		transport := &http.Transport{
			Proxy:           http.ProxyFromEnvironment,
			TLSClientConfig: opts.oauthTLSConfig,
		}
		oauthClient = &http.Client{
			Transport:     transport,
			Timeout:       defaultOAuthTimeout,
			CheckRedirect: sameOriginRedirectsOnly,
		}
		closeIdleConnections = transport.CloseIdleConnections
	}
	ctx = context.WithValue(ctx, oauth2.HTTPClient, oauthClient)

	return &Oauth2AuthManager{
		tokenSource: cfg.TokenSource(ctx),
	}, closeIdleConnections
}

func (r *Catalog) init(ctx context.Context, ops *options, uri string) error {
	baseuri, err := url.Parse(uri)
	if err != nil {
		return err
	}

	r.baseURI = baseuri.JoinPath("v1")

	// Capture the client-supplied properties before fetchConfig folds in the
	// server's /v1/config defaults and overrides, so the metrics reporter is
	// resolved from the client's choice alone (see the reporterProps field).
	r.reporterProps = toProps(ops)

	if ops, err = r.fetchConfig(ctx, ops); err != nil {
		return err
	}

	r.cl, r.closeSession, err = r.createSession(ctx, ops)
	if err != nil {
		return err
	}
	if ops.prefix != "" {
		r.baseURI = r.baseURI.JoinPath(ops.prefix)
	}
	r.props = toProps(ops)

	// Stand up the metrics dispatcher only when the client opted in and the
	// server advertises the endpoint. Enablement of outbound telemetry is read
	// from the client-supplied properties alone (see reporterProps): a server
	// must never be able to turn on reporting the client never asked for.
	//
	// Disabling, however, is always safe, so an explicit server-side false is
	// honored: r.props is the merged config (server defaults < client < server
	// overrides), so an override setting the key false resolves to false here and
	// suppresses a client that opted in — matching how a Java client resolves it
	// and letting an operator switch reporting off fleet-wide. A mere server
	// default cannot flip off a client opt-in, since client props beat defaults in
	// the merge.
	if reportMetricsEnabled(r.reporterProps) && reportMetricsEnabled(r.props) &&
		r.endpoints.check(endpointReportMetrics) == nil {
		r.metricsDispatcher = newMetricsDispatcher(
			metricsDispatchWorkers, metricsDispatchQueueSize, reportMetricsTimeout(r.reporterProps), nil)
	}

	return nil
}

// builtinHeaderDefaults returns the headers every session sends by default.
// They identify the client and carry no credentials, so they are the only
// defaults a request to an unconfigured origin receives (see
// sessionTransport.builtinHeaders).
func builtinHeaderDefaults() []struct{ key, value string } {
	return []struct{ key, value string }{
		{"X-Client-Version", icebergRestSpecVersion},
		{"Content-Type", "application/json"},
		{"User-Agent", "GoIceberg/" + iceberg.Version()},
		{headerIcebergAccessDelegation, defaultAccessDelegation},
	}
}

// createSession returns a cleanup that closes only transports created by this
// function, never transports supplied by the caller.
func (r *Catalog) createSession(ctx context.Context, opts *options) (*http.Client, func(), error) {
	var baseTransport http.RoundTripper
	var cleanupFuncs []func()
	if opts.transport != nil {
		baseTransport = opts.transport
	} else if opts.transportFactory != nil {
		var cleanup func()
		baseTransport, cleanup = opts.transportFactory(opts.tlsConfig)
		if cleanup != nil {
			cleanupFuncs = append(cleanupFuncs, cleanup)
		}
	} else {
		transport := &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: opts.tlsConfig}
		baseTransport = transport
		cleanupFuncs = append(cleanupFuncs, transport.CloseIdleConnections)
	}
	cleanup := func() {
		for _, cleanupFunc := range slices.Backward(cleanupFuncs) {
			cleanupFunc()
		}
	}

	session := &sessionTransport{
		RoundTripper:   baseTransport,
		defaultHeaders: http.Header{},
		catalogOrigin:  r.baseURI,
		authOrigin:     opts.authUri,
	}
	cl := &http.Client{Transport: session, CheckRedirect: sameOriginRedirectsOnly}

	// If the user does not set an AuthManager, construct one for this session
	// without storing it in opts. Bootstrap authentication must not leak into
	// the final session after server configuration has been applied.
	authManager := opts.authManager
	if authManager == nil {
		var cleanupOAuth func()
		authManager, cleanupOAuth = setupOAuthManager(r, cl, opts)
		if cleanupOAuth != nil {
			cleanupFuncs = append(cleanupFuncs, cleanupOAuth)
		}
	}

	builtins := builtinHeaderDefaults()
	for _, h := range builtins {
		session.defaultHeaders.Set(h.key, h.value)
	}

	for k, v := range opts.headers {
		session.defaultHeaders.Set(k, v)
	}

	for k, v := range opts.additionalProps {
		if headerKey, found := strings.CutPrefix(k, "header."); found {
			session.defaultHeaders.Set(headerKey, v)
		}
	}

	session.builtinHeaders = http.Header{}
	for _, h := range builtins {
		if v := session.defaultHeaders.Values(h.key); len(v) > 0 {
			session.builtinHeaders[http.CanonicalHeaderKey(h.key)] = slices.Clone(v)
		}
	}

	if authManager != nil {
		session.authManager = authManager
	}

	signer, err := resolveSigner(ctx, opts)
	if err != nil {
		cleanup()

		return nil, nil, err
	}
	session.signer = signer

	return cl, cleanup, nil
}

func (r *Catalog) fetchConfig(ctx context.Context, opts *options) (*options, error) {
	params := url.Values{}
	if opts.warehouseLocation != "" {
		params.Set(keyWarehouseLocation, opts.warehouseLocation)
	}

	route := r.baseURI.JoinPath("config")
	route.RawQuery = params.Encode()

	sess, cleanup, err := r.createSession(ctx, opts)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	rsp, err := doGet[configResponse](ctx, route, []string{}, sess, nil)
	if err != nil {
		return nil, err
	}

	cfg := rsp.Defaults
	if cfg == nil {
		cfg = iceberg.Properties{}
	}

	// Collapse the oauth2-server-uri / rest.authorization-url alias within each
	// layer before merging so the defaults -> client -> overrides precedence is
	// preserved. Merging first would leave both keys in the map, letting a
	// client oauth2-server-uri shadow a server rest.authorization-url override.
	clientProps := toProps(opts)
	resolveAuthURLAlias(cfg)
	resolveAuthURLAlias(clientProps)
	resolveAuthURLAlias(rsp.Overrides)

	maps.Copy(cfg, clientProps)
	maps.Copy(cfg, rsp.Overrides)

	r.namespaceSeparator = cfg.Get(keyNamespaceSeparator, defaultNamespaceSeparator)
	r.snapshotMode = SnapshotMode(cfg.Get(keySnapshotLoadingMode, ""))
	if r.snapshotMode != "" && r.snapshotMode != SnapshotModeAll && r.snapshotMode != SnapshotModeRefs {
		return nil, fmt.Errorf("%w: invalid %s %q (want %q or %q)",
			ErrRESTError, keySnapshotLoadingMode, r.snapshotMode, SnapshotModeAll, SnapshotModeRefs)
	}
	if r.tables, err = newTableCache(cfg); err != nil {
		return nil, err
	}

	// Negotiate capabilities from the endpoints the server advertises, falling
	// back to a backward-compatible default set when none are provided.
	r.endpoints = resolveEndpoints(rsp.Endpoints, cfg.GetBool(keyViewEndpointsSupported, false))

	o := *opts
	if err := fromProps(cfg, &o); err != nil {
		return nil, err
	}

	if uri, ok := cfg["uri"]; ok {
		r.baseURI, err = url.Parse(uri)
		if err != nil {
			return nil, err
		}
		r.baseURI = r.baseURI.JoinPath("v1")
	}

	return &o, nil
}

func (r *Catalog) Name() string              { return r.name }
func (r *Catalog) CatalogType() catalog.Type { return catalog.REST }

// Close drains idle connections from transports created by the catalog and
// releases its metrics reporter. Closing the metrics dispatcher cancels any
// in-flight reports and drains its workers so outbound telemetry stops.
// Caller-provided transports remain caller-owned. Close is safe to call more
// than once. Callers holding a [catalog.Catalog] can reach this via a
// [catalog.Closer] type assertion.
func (r *Catalog) Close() error {
	r.closeSessionOnce.Do(func() {
		if r.closeSession != nil {
			r.closeSession()
		}
		if r.metricsDispatcher != nil {
			r.metricsDispatcher.close()
		}
		r.closeErr = r.reporter.Close()
		r.tables.purge()
	})

	return r.closeErr
}

var _ catalog.Closer = (*Catalog)(nil)

func (r *Catalog) checkValidNamespace(ident table.Identifier) error {
	if err := catalog.ValidateNamespaceIdentifier(ident); err != nil {
		return err
	}

	separator := r.decodedNamespaceSeparator()
	if separator == "" {
		return nil
	}

	for _, part := range ident {
		if strings.Contains(part, separator) {
			return fmt.Errorf("%w: namespace component %q contains the namespace separator %q in %v",
				catalog.ErrNoSuchNamespace, part, separator, strings.Join(ident, "."))
		}
	}

	return nil
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

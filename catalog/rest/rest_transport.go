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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go"
)

type errorResponse struct {
	Message string   `json:"message"`
	Type    string   `json:"type"`
	Code    int      `json:"code"`
	Stack   []string `json:"stack,omitempty"`

	wrapping   error
	statusCode int
	retryAfter string
}

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

type sessionTransport struct {
	http.RoundTripper

	authManager    AuthManager
	defaultHeaders http.Header
	signer         RequestSigner

	// builtinHeaders is the subset of defaultHeaders under the built-in keys
	// (with any operator override applied), which identify the client and
	// carry no credentials. It is all a request to an origin other than
	// catalogOrigin or authOrigin (e.g. a redirect hop) receives.
	builtinHeaders http.Header
	// catalogOrigin is the configured catalog origin: the only origin that
	// receives the auth header and the signer's signature. authOrigin is the
	// configured OAuth token endpoint, if any, which also receives
	// user-supplied default headers.
	// These are compared against the request URL rather than derived from
	// the redirect chain, so a transport that omits Response.Request cannot
	// widen them. A nil catalogOrigin trusts no origin.
	catalogOrigin *url.URL
	authOrigin    *url.URL
}

// maxRedirects matches net/http's default redirect policy, which a custom
// CheckRedirect replaces.
const maxRedirects = 10

// sameOriginRedirectsOnly is the CheckRedirect policy for every client the
// catalog builds. A redirect is followed only while it stays on the origin of
// the request that started the chain. A cross-origin hop would otherwise
// replay a 307/308 body (such as the client_credentials form, client_secret
// included) to the other origin, and a hop that redirects back to a different
// catalog path would be sent with the catalog's credentials.
func sameOriginRedirectsOnly(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	if from := via[0].URL; !sameOrigin(from, req.URL) {
		return fmt.Errorf("%w: refusing redirect from %s://%s to a different origin %s://%s",
			ErrRESTError, from.Scheme, from.Host, req.URL.Scheme, req.URL.Host)
	}

	return nil
}

// sameOrigin reports whether two URLs share scheme, host, and effective port.
func sameOrigin(a, b *url.URL) bool {
	return strings.EqualFold(a.Scheme, b.Scheme) &&
		strings.EqualFold(a.Hostname(), b.Hostname()) &&
		defaultedPort(a) == defaultedPort(b)
}

func defaultedPort(u *url.URL) string {
	if p := u.Port(); p != "" {
		return p
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return "443"
	case "http":
		return "80"
	default:
		return ""
	}
}

func (s *sessionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// This transport adds the auth header, the signature and the session
	// defaults to every request it sends, including each redirect hop.
	// net/http's redirect stripping only covers Authorization and cookies set
	// on the request passed to Client.Do, and never custom headers, so these
	// are applied only for the configured origins. sameOriginRedirectsOnly
	// keeps the clients from following a cross-origin hop at all; this gate
	// is the second line of defense.
	toCatalog := s.catalogOrigin != nil && sameOrigin(s.catalogOrigin, r.URL)
	defaults := s.builtinHeaders
	if toCatalog || (s.authOrigin != nil && sameOrigin(s.authOrigin, r.URL)) {
		defaults = s.defaultHeaders
	}

	// A session default is applied unless the request already carries that
	// header (a per-request override of any default, not just Content-Type
	// wins) or explicitly opted out of it via withSuppressedHeaders (carried on
	// the context as an explicit set, never inferred from header values).
	suppressed := suppressedHeadersFrom(r.Context())
	for k, v := range defaults {
		ck := http.CanonicalHeaderKey(k)
		if _, ok := r.Header[ck]; ok {
			continue
		}
		if _, ok := suppressed[ck]; ok {
			continue
		}
		for _, hdr := range v {
			r.Header.Add(k, hdr)
		}
	}

	// Ordering is load-bearing: authManager runs after the default-header loop
	// and Set-overwrites, so the managed auth header always wins even though the
	// loop above lets an arbitrary per-request header (withHeaders) override a
	// session default of the same key. A caller cannot suppress or spoof the
	// Authorization header by supplying its own. Do not reorder this before the
	// default-header loop.
	if s.authManager != nil && toCatalog && r.Context().Value(skipOAuth) == nil {
		var (
			k, v string
			err  error
		)
		// Prefer the context-aware path so the request deadline also bounds auth,
		// falling back to the context-free method for managers that do not
		// implement ContextAuthManager.
		if cm, ok := s.authManager.(ContextAuthManager); ok {
			k, v, err = cm.AuthHeaderWithContext(r.Context())
		} else {
			k, v, err = s.authManager.AuthHeader()
		}
		if err != nil {
			return nil, err
		}
		r.Header.Set(k, v)
	}

	// A signer only signs requests to the configured catalog origin: a request
	// to a different origin (e.g. a redirect hop) is left unsigned, so the
	// signer's Authorization header and any session token never reach an
	// unconfigured host. The guard lives here, in core, so it covers every
	// signer, including one installed verbatim via WithSigner.
	if s.signer != nil && toCatalog {
		if err := s.signer.SignRequest(r); err != nil {
			return nil, err
		}
	}

	return s.RoundTripper.RoundTrip(r)
}

// suppressHeadersKey carries the set of session-default header keys (canonical
// form) that a request opted out of, so sessionTransport.RoundTrip can consult
// an explicit set rather than inferring suppression from header values.
type suppressHeadersKey struct{}

// reqConfig holds the optional per-request knobs shared by do and doPost:
// headers to set, session-default headers to suppress, a body error.type ->
// error override, and whether an HTTP 204 is a valid empty result.
type reqConfig struct {
	headers           map[string]string
	suppressHeaders   []string
	errorTypeOverride map[string]error
	allowNoContent    bool
	requireBody       bool
	queryParams       url.Values
	// allowNotModified turns a 304 into errNotModified. withIfNoneMatch sets it
	// together with the header, so a 304 to a request that sent no ETag stays an
	// error.
	allowNotModified bool
	onHeaders        func(http.Header)
}

type reqOption func(*reqConfig)

// withHeaders sets per-request headers, merging with any already set so options
// compose. A per-request header takes precedence over a session default because
// sessionTransport.RoundTrip skips any default whose key is already present.
func withHeaders(headers map[string]string) reqOption {
	return func(c *reqConfig) {
		if c.headers == nil {
			c.headers = make(map[string]string, len(headers))
		}
		maps.Copy(c.headers, headers)
	}
}

// withSuppressedHeaders prevents the named session-default headers from being
// sent on this request. Names accumulate so options compose. Suppression is
// carried through the request context (see RoundTrip), not encoded as a header
// value, so req.Header only ever means "headers to send".
func withSuppressedHeaders(names ...string) reqOption {
	return func(c *reqConfig) { c.suppressHeaders = append(c.suppressHeaders, names...) }
}

// withErrorTypeOverride maps a non-200 response to a specific error by the REST
// error.type in the response body. It takes precedence over the status-code
// override so a shared status (e.g. a 404 that can be a missing plan, table, or
// namespace) can split into distinct sentinels. Entries accumulate so options
// compose; an unset or unmatched error.type falls through to the status-code
// override.
func withErrorTypeOverride(byType map[string]error) reqOption {
	return func(c *reqConfig) {
		if c.errorTypeOverride == nil {
			c.errorTypeOverride = make(map[string]error, len(byType))
		}
		maps.Copy(c.errorTypeOverride, byType)
	}
}

// allowNoContent treats an HTTP 204 No Content response as a valid empty result
// rather than trying to decode a body.
func allowNoContent() reqOption {
	return func(c *reqConfig) { c.allowNoContent = true }
}

// requireBody rejects an HTTP 200 with an empty body rather than decoding it to
// the zero result. Use it when a 200 must carry a payload, so a truncated or
// proxy-stripped response cannot masquerade as a successful empty result (e.g.
// fetchScanTasks returning "zero tasks" and silently dropping work).
func requireBody() reqOption {
	return func(c *reqConfig) { c.requireBody = true }
}

// withIfNoneMatch sends If-None-Match with etag and reports a 304 Not
// Modified response as errNotModified instead of an error response.
func withIfNoneMatch(etag string) reqOption {
	return func(c *reqConfig) {
		withHeaders(map[string]string{"If-None-Match": etag})(c)
		c.allowNotModified = true
	}
}

// withResponseHeaders passes the headers of a 200 response to fn.
func withResponseHeaders(fn func(http.Header)) reqOption {
	return func(c *reqConfig) { c.onHeaders = fn }
}

// Appends query parameters to the request.
func withQueryParams(params url.Values) reqOption {
	return func(c *reqConfig) {
		if c.queryParams == nil {
			c.queryParams = make(url.Values, len(params))
		}
		maps.Copy(c.queryParams, params)
	}
}

func newReqConfig(opts []reqOption) reqConfig {
	var cfg reqConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	return cfg
}

// withSuppressedHeadersCtx returns ctx carrying the canonicalized suppression
// set, or ctx unchanged when nothing is suppressed.
func withSuppressedHeadersCtx(ctx context.Context, names []string) context.Context {
	if len(names) == 0 {
		return ctx
	}

	set := make(map[string]struct{}, len(names))
	for _, n := range names {
		set[http.CanonicalHeaderKey(n)] = struct{}{}
	}

	return context.WithValue(ctx, suppressHeadersKey{}, set)
}

func suppressedHeadersFrom(ctx context.Context) map[string]struct{} {
	set, _ := ctx.Value(suppressHeadersKey{}).(map[string]struct{})

	return set
}

func do[T any](ctx context.Context, method string, baseURI *url.URL, path []string, cl *http.Client, override map[int]error, opts ...reqOption) (ret T, err error) {
	cfg := newReqConfig(opts)

	var (
		req *http.Request
		rsp *http.Response
	)

	u := baseURI.JoinPath(path...)
	if len(cfg.queryParams) > 0 {
		q := u.Query()
		for k, vs := range cfg.queryParams {
			for _, v := range vs {
				q.Add(k, v)
			}
		}
		u.RawQuery = q.Encode()
	}
	uri := u.String()
	ctx = withSuppressedHeadersCtx(ctx, cfg.suppressHeaders)
	if req, err = http.NewRequestWithContext(ctx, method, uri, nil); err != nil {
		return ret, err
	}
	setRequestHeaders(req, cfg.headers)

	if rsp, err = cl.Do(req); err != nil {
		return ret, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, rsp.Body)
		_ = rsp.Body.Close()
	}()

	if cfg.allowNoContent && rsp.StatusCode == http.StatusNoContent {
		return ret, err
	}

	if cfg.allowNotModified && rsp.StatusCode == http.StatusNotModified {
		return ret, errNotModified
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override, cfg.errorTypeOverride)
	}

	if cfg.onHeaders != nil {
		cfg.onHeaders(rsp.Header)
	}

	if method == http.MethodHead || method == http.MethodDelete {
		return ret, err
	}

	if err = decodeJSONResponse(rsp.Body, &ret); err != nil {
		return ret, err
	}

	return ret, err
}

func doGet[T any](ctx context.Context, baseURI *url.URL, path []string, cl *http.Client, override map[int]error, opts ...reqOption) (ret T, err error) {
	return do[T](ctx, http.MethodGet, baseURI, path, cl, override, opts...)
}

func doDelete[T any](ctx context.Context, baseURI *url.URL, path []string, cl *http.Client, override map[int]error, opts ...reqOption) (ret T, err error) {
	return do[T](ctx, http.MethodDelete, baseURI, path, cl, override, append([]reqOption{allowNoContent()}, opts...)...)
}

func doHead(ctx context.Context, baseURI *url.URL, path []string, cl *http.Client, override map[int]error) error {
	_, err := do[struct{}](ctx, http.MethodHead, baseURI, path, cl, override, allowNoContent())

	return err
}

func doPost[Payload, Result any](ctx context.Context, baseURI *url.URL, path []string, payload Payload, cl *http.Client, override map[int]error, opts ...reqOption) (ret Result, err error) {
	cfg := newReqConfig(opts)

	var (
		req  *http.Request
		rsp  *http.Response
		data []byte
	)

	uri := baseURI.JoinPath(path...).String()
	data, err = json.Marshal(payload)
	if err != nil {
		return ret, err
	}

	ctx = withSuppressedHeadersCtx(ctx, cfg.suppressHeaders)
	req, err = http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(data))
	if err != nil {
		return ret, err
	}
	setRequestHeaders(req, cfg.headers)

	rsp, err = cl.Do(req)
	if err != nil {
		return ret, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, rsp.Body)
		_ = rsp.Body.Close()
	}()

	if cfg.allowNoContent && rsp.StatusCode == http.StatusNoContent {
		return ret, err
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override, cfg.errorTypeOverride)
	}

	if rsp.ContentLength == 0 {
		if cfg.requireBody {
			return ret, fmt.Errorf("%w: empty response body on %s %s", ErrRESTError, req.Method, req.URL.Path)
		}

		return ret, err
	}

	if err = decodeJSONResponse(rsp.Body, &ret); err != nil {
		return ret, err
	}

	return ret, err
}

func decodeJSONResponse(body io.Reader, dst any) error {
	decoder := json.NewDecoder(body)
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	if err := rejectTrailingJSON(decoder); err != nil {
		return fmt.Errorf("%w: %s", ErrRESTError, err)
	}

	return nil
}

func rejectTrailingJSON(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return errors.New("response contains multiple JSON values")
		}

		return fmt.Errorf("error decoding trailing json payload: `%s`", err.Error())
	}

	return nil
}

func setRequestHeaders(req *http.Request, headers map[string]string) {
	for k, v := range headers {
		req.Header.Set(k, v)
	}
}

func handleNon200(rsp *http.Response, override map[int]error, typeOverride map[string]error) error {
	e := errorResponse{
		statusCode: rsp.StatusCode,
		retryAfter: rsp.Header.Get("Retry-After"),
	}

	// Only try to decode if there's a body (HEAD requests don't have one)
	if rsp.ContentLength != 0 {

		payload := struct {
			Error   *errorResponse `json:"error"`
			Message string         `json:"message"`
			Type    string         `json:"type"`
		}{
			Error: &e,
		}

		decoder := json.NewDecoder(rsp.Body)
		decErr := decoder.Decode(&payload)
		if decErr == nil {
			decErr = rejectTrailingJSON(decoder)
		}
		if decErr != nil && decErr != io.EOF {
			// Preserve the HTTP metadata even when the server returned a non-JSON
			// error page. Callers such as WaitForPlan still need the status to apply
			// transport-level retry policy. A status the caller mapped keeps that
			// classification (e.g. an ambiguous commit 5xx stays ErrCommitStateUnknown)
			// even when a proxy garbled the body; unmapped statuses keep ErrRESTError.
			e.wrapping = ErrRESTError
			if statusErr, ok := override[rsp.StatusCode]; ok {
				e.wrapping = statusErr
			}

			return fmt.Errorf("%w: failed to decode error response: %s", e, decErr.Error())
		}

		if e.Message == "" && e.Type == "" {
			e.Message = payload.Message
			e.Type = payload.Type
		}
	}

	// The status-code override maps a status to a sentinel; a body error.type
	// override refines it further (e.g. splitting a 404 into missing-plan vs.
	// table vs. namespace). The type refinement only applies to a status the
	// caller already mapped, because these error.type values are defined for
	// specific statuses (404) in the REST spec: a 503 body carrying, say,
	// NoSuchPlanIdException must still resolve to ErrServiceUnavailable rather
	// than a 404 sentinel.
	if override != nil {
		if statusErr, ok := override[rsp.StatusCode]; ok {
			if typeOverride != nil && e.Type != "" {
				if typeErr, ok := typeOverride[e.Type]; ok {
					e.wrapping = typeErr

					return e
				}
			}
			e.wrapping = statusErr

			return e
		}
	}

	switch rsp.StatusCode {
	case http.StatusBadRequest:
		e.wrapping = ErrBadRequest
	case http.StatusUnauthorized:
		e.wrapping = ErrUnauthorized
	case http.StatusForbidden:
		e.wrapping = ErrForbidden
	case http.StatusUnprocessableEntity:
		e.wrapping = ErrRESTError
	case 419:
		e.wrapping = ErrAuthorizationExpired
	case http.StatusNotImplemented:
		e.wrapping = iceberg.ErrNotImplemented
	case http.StatusServiceUnavailable:
		e.wrapping = ErrServiceUnavailable
	default:
		if 500 <= rsp.StatusCode && rsp.StatusCode < 600 {
			e.wrapping = ErrServerError
		} else {
			e.wrapping = ErrRESTError
		}
	}

	return e
}

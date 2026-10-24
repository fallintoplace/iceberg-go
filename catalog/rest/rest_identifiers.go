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
	"fmt"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

type identifierPageFetcher func(pageToken string) ([]table.Identifier, string, error)

func paginateIdentifiers(fetch identifierPageFetcher, yield func(table.Identifier) bool) error {
	seenTokens := make(map[string]struct{})
	var pageToken string

	for {
		identifiers, nextPageToken, err := fetch(pageToken)
		if err != nil {
			return err
		}
		for _, identifier := range identifiers {
			if !yield(identifier) {
				return nil
			}
		}
		if nextPageToken == "" {
			return nil
		}
		if _, ok := seenTokens[nextPageToken]; ok {
			return fmt.Errorf("%w: pagination cycle: repeated page token", ErrRESTError)
		}

		seenTokens[nextPageToken] = struct{}{}
		pageToken = nextPageToken
	}
}

func (r *Catalog) nsSeparator() string {
	if r.namespaceSeparator == "" {
		return defaultNamespaceSeparator
	}

	return r.namespaceSeparator
}

// encodePathSegment escapes a REST path segment per RFC 3986. PathEscape
// leaves plus signs and pure-dot segments literal, so encode them explicitly to
// avoid form decoders interpreting plus signs as spaces and JoinPath resolving
// dot segments while cleaning the path.
func encodePathSegment(value string) string {
	switch value {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return strings.ReplaceAll(url.PathEscape(value), "+", "%2B")
	}
}

// encodeNamespace URL-encodes each namespace level and joins them with the
// server-advertised, URL-encoded namespace separator for use as a REST path
// segment.
func (r *Catalog) encodeNamespace(namespace table.Identifier) string {
	encoded := make([]string, len(namespace))
	for i, level := range namespace {
		encoded[i] = encodePathSegment(level)
	}

	return strings.Join(encoded, r.nsSeparator())
}

func (r *Catalog) decodedNamespaceSeparator() string {
	sep, err := url.PathUnescape(r.nsSeparator())
	if err != nil {
		return r.nsSeparator()
	}

	return sep
}

// namespaceToQueryParam joins the raw namespace levels with the decoded
// separator for use as a query-parameter value, which the HTTP layer then
// percent-encodes. Mirrors RESTUtil.namespaceToQueryParam in Java.
func (r *Catalog) namespaceToQueryParam(namespace table.Identifier) string {
	return strings.Join(namespace, r.decodedNamespaceSeparator())
}

// identifierPath keeps rawName for JSON request bodies separate from
// encodedNamespace and encodedName for URL paths. The validated table, view, and
// function path builders return a zero value only alongside a non-nil error.
type identifierPath struct {
	encodedNamespace string
	rawName          string
	encodedName      string
}

// namespaceReqPath builds a namespace-scoped path, such as a create or register
// endpoint, without including the object name.
func (p identifierPath) namespaceReqPath(ep endpoint) ([]string, error) {
	return ep.reqPath(p.encodedNamespace)
}

// objectReqPath builds an object-scoped path using the encoded identifier components.
// rawExtra contains raw opaque path segments, such as scan plan IDs. Encode each
// segment here so slashes, plus signs, and pure-dot segments remain literal.
func (p identifierPath) objectReqPath(ep endpoint, rawExtra ...string) ([]string, error) {
	params := make([]string, 0, 2+len(rawExtra))
	params = append(params, p.encodedNamespace, p.encodedName)
	for _, seg := range rawExtra {
		params = append(params, encodePathSegment(seg))
	}

	return ep.reqPath(params...)
}

// newIdentifierPath builds path components from an already validated identifier.
func (r *Catalog) newIdentifierPath(ident table.Identifier) identifierPath {
	rawName := catalog.ObjectNameFromIdent(ident)

	return identifierPath{
		encodedNamespace: r.encodeNamespace(catalog.NamespaceFromIdent(ident)),
		rawName:          rawName,
		encodedName:      encodePathSegment(rawName),
	}
}

func (r *Catalog) tableIdentifierPath(ident table.Identifier) (identifierPath, error) {
	if err := catalog.ValidateTableIdentifier(ident); err != nil {
		return identifierPath{}, err
	}

	return r.newIdentifierPath(ident), nil
}

func (r *Catalog) viewIdentifierPath(ident table.Identifier) (identifierPath, error) {
	if err := catalog.ValidateViewIdentifier(ident); err != nil {
		return identifierPath{}, err
	}

	return r.newIdentifierPath(ident), nil
}

func (r *Catalog) functionIdentifierPath(ident table.Identifier) (identifierPath, error) {
	if err := catalog.ValidateFunctionIdentifier(ident); err != nil {
		return identifierPath{}, err
	}

	return r.newIdentifierPath(ident), nil
}

func (r *Catalog) getPageSize(ctx context.Context) int {
	if pageSize, ok := ctx.Value(pageSizeKey).(int); ok {
		return pageSize
	}

	return defaultPageSize
}

func (r *Catalog) SetPageSize(ctx context.Context, sz int) context.Context {
	return context.WithValue(ctx, pageSizeKey, sz)
}

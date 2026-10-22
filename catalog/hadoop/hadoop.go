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

// Package hadoop implements a catalog over a Hadoop-style warehouse directory.
//
// It links no cloud SDK. Callers on cloud storage must blank-import the backend for their scheme.
// For example io/gocloud/s3.
//
// Otherwise the first LoadFS for a cloud path fails with io.ErrIOSchemeNotFound naming the package to import.
package hadoop

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/metrics"
	"github.com/apache/iceberg-go/table"
)

func init() {
	catalog.Register(string(catalog.Hadoop), catalog.RegistrarFunc(
		func(ctx context.Context, name string, props iceberg.Properties) (catalog.Catalog, error) {
			warehouse := props.Get("warehouse", "")

			return NewCatalog(ctx, name, warehouse, props)
		},
	))
}

var _ catalog.Catalog = (*Catalog)(nil)

func shouldSuppressPermissionError(err error) bool {
	return errors.Is(err, fs.ErrPermission) ||
		(err != nil && strings.Contains(err.Error(), "AuthorizationPermissionMismatch"))
}

// validateIdentifier checks that an identifier is non-empty and that each
// component is safe for use as a path segment. It rejects nil/empty
// identifiers, empty parts, path traversal sequences, and components
// containing path separators.
//
// Note: this is POSIX-best-effort validation — it does not catch NUL bytes
// or Windows reserved names (NUL, CON, COM1, etc.).
func validateIdentifier(ident table.Identifier) error {
	return validateIdentifierParts(ident, "namespace identifier", "identifier component")
}

func validateTableIdentifier(ident table.Identifier) error {
	if len(ident) < 2 {
		return fmt.Errorf("%w: table identifier must have at least a namespace and table name", catalog.ErrInvalidIdentifier)
	}

	return validateIdentifierParts(ident, "table identifier", "table identifier component")
}

func validateIdentifierParts(ident table.Identifier, identifierName, componentName string) error {
	if len(ident) == 0 {
		return fmt.Errorf("%w: %s must not be empty", catalog.ErrInvalidIdentifier, identifierName)
	}

	for _, part := range ident {
		if part == "" {
			return fmt.Errorf("%w: %s must not be empty", catalog.ErrInvalidIdentifier, componentName)
		}

		if part == "." || part == ".." {
			return fmt.Errorf("%w: invalid %s %q", catalog.ErrInvalidIdentifier, componentName, part)
		}

		if strings.ContainsAny(part, "/\\") {
			return fmt.Errorf("%w: %s must not contain path separators: %q", catalog.ErrInvalidIdentifier, componentName, part)
		}
	}

	return nil
}

// Catalog is a filesystem-based Iceberg catalog that requires no external
// metastore. All state lives on disk as directories and versioned JSON
// metadata files. Currently only local filesystem paths are supported.
var _ catalog.PurgeableTable = (*Catalog)(nil)

type Catalog struct {
	name       string
	warehouse  string
	isLocal    bool
	filesystem HadoopCatalogFS
	props      iceberg.Properties
	// reporter builds and caches the catalog's metrics reporter once, so it is
	// constructed per-catalog rather than per table load. Released by Close.
	reporter metrics.CachedReporter
}

// NewCatalog creates a new Hadoop catalog rooted at the given warehouse path.
// The context is used to initialize the filesystem so callers can cancel setup.
// When using a local filesystem, the warehouse directory
// is not created on construction; it is created implicitly by the first CreateNamespace
// call. When using other schemes, the property `allow-unsafe-commits` must be set to
// true since custom schemes and blob filesystems do not have the same atomicity guarantees.
func NewCatalog(ctx context.Context, name, warehouse string, props iceberg.Properties) (*Catalog, error) {
	if warehouse == "" {
		return nil, errors.New("hadoop catalog requires a warehouse path")
	}

	u, err := url.Parse(warehouse)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: invalid warehouse path: %w", err)
	}

	isLocal := u.Scheme == "" || u.Scheme == "file"
	allowUnsafeCommits := props.GetBool("allow-unsafe-commits", false)

	if !isLocal && !allowUnsafeCommits {
		return nil, fmt.Errorf("hadoop catalog: when using warehouse scheme %q, `allow-unsafe-commits` must be set to true; this implementation is not atomic and can lose data with concurrent commits", u.Scheme)
	}

	if isLocal {
		if u.Opaque != "" {
			warehouse = u.Opaque
		} else {
			warehouse = u.Path
		}

		if warehouse == "" || warehouse == "/" {
			return nil, errors.New("hadoop catalog: local filesystem requires a non-root warehouse path")
		}

		warehouse = strings.TrimRight(warehouse, "/")

		// Normalize to absolute path so the synthetic "location" property
		// always produces a valid file:// URI.
		absWarehouse, err := filepath.Abs(warehouse)
		if err != nil {
			return nil, fmt.Errorf("hadoop catalog: failed to resolve absolute warehouse path: %w", err)
		}

		warehouse = absWarehouse
	}
	filesystem, err := io.LoadFS(ctx, props, warehouse)
	if err != nil {
		return nil, fmt.Errorf("hadoop catalog: failed to load filesystem: %w", err)
	}

	hadoopFs, ok := filesystem.(HadoopCatalogFS)
	if !ok {
		return nil, fmt.Errorf("hadoop catalog: %T does not implement HadoopCatalogFS", filesystem)
	}

	cat := &Catalog{
		name:      name,
		warehouse: warehouse,
		isLocal:   isLocal,
		// filesystem is resolved dynamically from the IO registry based on the warehouse scheme
		filesystem: hadoopFs,
		props:      props,
	}

	// Resolve the metrics reporter once, up front. CachedReporter caches the
	// first result, so building it here means a bad metrics-reporter-impl fails
	// construction — rather than surfacing later as a spurious CreateTable error
	// after the table is already on disk, or as a cached error that wedges every
	// later table op on this catalog.
	if _, err := cat.reporter.Get(props); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics reporter: %w", err)
	}

	return cat, nil
}

func (c *Catalog) CatalogType() catalog.Type {
	return catalog.Hadoop
}

// Close releases the catalog's metrics reporter. Callers holding a
// [catalog.Catalog] can reach this via a [catalog.Closer] type assertion.
func (c *Catalog) Close() error { return c.reporter.Close() }

var _ catalog.Closer = (*Catalog)(nil)

// joinPath is a helper that allows paths to be joined as both local filesystem
// paths or as remote URIs needed for filesystems like blob stores.
func joinPath(isLocal bool, base string, parts ...string) string {
	if isLocal {
		// Local filesystems can be joined using path.Join
		// without any special handling.
		return filepath.Join(append([]string{base}, parts...)...)
	}

	baseWithoutTrailingSlash := strings.TrimRight(base, "/")
	if len(parts) == 0 {
		// Remote warehouse roots should not keep a trailing slash.
		return baseWithoutTrailingSlash
	}

	// Remote paths need POSIX separators without removing the URI authority.
	joinedParts := path.Join(parts...)
	// joined returns . if all parts are empty; if this is the case,
	// we return the base without the trailing slash
	if joinedParts == "." {
		return baseWithoutTrailingSlash
	}

	return baseWithoutTrailingSlash + "/" + joinedParts
}

func (c *Catalog) namespaceToPath(ns table.Identifier) string {
	return joinPath(c.isLocal, c.warehouse, ns...)
}

func (c *Catalog) tableToPath(ident table.Identifier) string {
	return joinPath(c.isLocal, c.warehouse, ident...)
}

func (c *Catalog) defaultTableLocation(ident table.Identifier) string {
	return c.tableToPath(ident)
}


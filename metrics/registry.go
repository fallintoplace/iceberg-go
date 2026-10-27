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

package metrics

import (
	"fmt"
	"maps"
	"sync"
)

// ReporterImplKey is the catalog/table property that selects a registered
// reporter by name (e.g. "logging"). It is the Go analogue of Java's
// metrics-reporter-impl; Go uses a name→factory registry rather than
// reflection over a class name.
const ReporterImplKey = "metrics-reporter-impl"

// Built-in reporter names usable as the value of [ReporterImplKey].
const (
	ReporterNameNop     = "nop"
	ReporterNameLogging = "logging"
)

// Factory builds a Reporter from configuration properties. The same property
// map that selected the reporter is passed in, so a factory may read its own
// configuration keys.
//
// A factory must return either a usable [Reporter] or a non-nil error, never a
// typed-nil reporter (e.g. (*Custom)(nil)): that is a non-nil interface value,
// so it passes interface nil checks and its Report/Close would nil-deref where a
// caller reasonably assumed a nil interface meant "no reporter".
type Factory func(props map[string]string) (Reporter, error)

// Registry holds reporter factories for use in an application or test.
// Registrations are safe for concurrent access. Factory calls run without
// holding the registry lock.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

func builtInRegistry() *Registry {
	return &Registry{factories: map[string]Factory{
		ReporterNameNop: func(map[string]string) (Reporter, error) {
			return NopReporter{}, nil
		},
		ReporterNameLogging: func(map[string]string) (Reporter, error) {
			return NewLoggingReporter(nil), nil
		},
	}}
}

// defaultRegistry retains the package-level registration behavior.
var defaultRegistry = builtInRegistry()

// NewRegistry returns an independent snapshot of the globally registered
// factories, including the built-in reporters. Registrations made after the
// call are not shared with the new registry.
func NewRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Clone copies factory registrations into a new, independent registry.
// The factory functions themselves are shared, not copied.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Registry{factories: maps.Clone(r.factories)}
}

// Register makes a reporter factory available under name in this registry.
// It panics if name is empty, factory is nil, or name is already registered,
// following the package-level Register behavior.
func (r *Registry) Register(name string, factory Factory) {
	if name == "" {
		panic("metrics: Register called with empty name")
	}
	if factory == nil {
		panic("metrics: Register called with nil factory")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, dup := r.factories[name]; dup {
		panic("metrics: Register called twice for " + name)
	}
	if r.factories == nil {
		r.factories = make(map[string]Factory)
	}
	r.factories[name] = factory
}

// Register makes a reporter factory available through the global registry.
// It panics if name is empty, factory is nil, or name is already registered,
// mirroring database/sql.Register.
func Register(name string, factory Factory) {
	defaultRegistry.Register(name, factory)
}

// Deregister removes a factory from this registry. Unregistered names are a
// no-op. Built-in reporters cannot be removed.
func (r *Registry) Deregister(name string) {
	if name == ReporterNameNop || name == ReporterNameLogging {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.factories, name)
}

// Deregister removes a factory from the global registry. Built-in reporters
// cannot be removed. Primarily useful for undoing registrations in tests.
func Deregister(name string) {
	defaultRegistry.Deregister(name)
}

// FromProperties builds the reporter selected by props[ReporterImplKey] from
// this registry. An absent or empty name yields NopReporter; an unknown name
// is an error. Factory calls run outside the registry lock.
func (r *Registry) FromProperties(props map[string]string) (Reporter, error) {
	name := props[ReporterImplKey]
	if name == "" {
		return NopReporter{}, nil
	}

	r.mu.RLock()
	factory, ok := r.factories[name]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("metrics: unknown reporter %q (set via %q)", name, ReporterImplKey)
	}

	return factory(props)
}

// FromProperties builds the reporter named by props[ReporterImplKey] from the
// global registry. An absent or empty name yields NopReporter (reporting is
// opt-in); an unrecognized name is an error.
//
// This nop default is an intentional divergence from Java, where an absent
// metrics-reporter-impl defaults to LoggingMetricsReporter. Set
// ReporterImplKey to ReporterNameLogging to opt into logging.
func FromProperties(props map[string]string) (Reporter, error) {
	return defaultRegistry.FromProperties(props)
}

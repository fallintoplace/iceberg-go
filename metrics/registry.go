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
	"errors"
	"fmt"
	"maps"
	"slices"
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

var (
	// ErrInvalidReporterRegistration means the name was empty or its factory nil.
	ErrInvalidReporterRegistration = errors.New("metrics: invalid reporter registration")
	// ErrReporterAlreadyRegistered means the name already has a factory.
	ErrReporterAlreadyRegistered = errors.New("metrics: reporter already registered")
)

// Registry manages independent reporter factory registrations. It is safe for
// concurrent use. The zero value is empty; NewRegistry includes built-ins.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

var defaultRegistry = &Registry{factories: map[string]Factory{
	ReporterNameNop: func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	},
	ReporterNameLogging: func(map[string]string) (Reporter, error) {
		return NewLoggingReporter(nil), nil
	},
}}

// NewRegistry returns an isolated snapshot of the global registrations,
// including built-ins. Future changes to either registry do not cross over.
func NewRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Clone copies registrations into an independent map and lock. Factory
// functions themselves are shared.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Registry{factories: maps.Clone(r.factories)}
}

// Names returns registered reporter names in sorted order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Sorted(maps.Keys(r.factories))
}

// Register adds a reporter factory and returns an error without mutating the
// registry for invalid names, nil factories, or duplicate names.
func (r *Registry) Register(name string, factory Factory) error {
	if name == "" {
		return fmt.Errorf("%w: name must not be empty", ErrInvalidReporterRegistration)
	}
	if factory == nil {
		return fmt.Errorf("%w: factory for %q is nil", ErrInvalidReporterRegistration, name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.factories[name]; exists {
		return fmt.Errorf("%w: %q", ErrReporterAlreadyRegistered, name)
	}
	if r.factories == nil {
		r.factories = make(map[string]Factory)
	}
	r.factories[name] = factory

	return nil
}

// MustRegister panics on registration failure. Use it during package init
// when missing a required reporter is an unrecoverable programming error.
func (r *Registry) MustRegister(name string, factory Factory) {
	if err := r.Register(name, factory); err != nil {
		panic(err)
	}
}

// Deregister removes a factory from this registry. Unknown names are ignored.
// Built-in names cannot be removed.
func (r *Registry) Deregister(name string) {
	if name == ReporterNameNop || name == ReporterNameLogging {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.factories, name)
}

// FromProperties resolves a reporter only through this registry. Missing names
// yield NopReporter. The factory executes after releasing the registry lock,
// so factories may recursively register other factories.
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

// Register adds a factory to the process-wide registry. Unlike earlier
// versions, it returns errors instead of panicking for invalid registrations.
// Use MustRegister in package init functions.
func Register(name string, factory Factory) error {
	return defaultRegistry.Register(name, factory)
}

// MustRegister registers a factory globally or panics on failure.
func MustRegister(name string, factory Factory) {
	defaultRegistry.MustRegister(name, factory)
}

// Deregister removes a factory globally, except for built-in names.
func Deregister(name string) {
	defaultRegistry.Deregister(name)
}

// FromProperties builds the reporter selected by props[ReporterImplKey] using
// the process-wide registry. An absent name intentionally selects NopReporter,
// unlike Java, to keep reporting opt-in.
func FromProperties(props map[string]string) (Reporter, error) {
	return defaultRegistry.FromProperties(props)
}

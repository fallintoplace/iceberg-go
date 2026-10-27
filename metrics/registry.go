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

var (
	// ErrReporterAlreadyRegistered indicates that a registry contains a factory
	// with the requested name.
	ErrReporterAlreadyRegistered = errors.New("metrics: reporter already registered")

	// ErrInvalidReporterRegistration indicates an empty name or nil factory.
	ErrInvalidReporterRegistration = errors.New("metrics: invalid reporter registration")
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

// Registry manages reporter factories independently of other registries.
// It is safe for concurrent use. Factories run without holding its lock,
// so they may register other reporters or construct nested reporters.
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

// NewRegistry snapshots globally registered reporter factories, including
// built-ins. Future changes to either registry do not affect the other.
// The zero value of Registry is also usable, but has no registered factories.
func NewRegistry() *Registry {
	return defaultRegistry.Clone()
}

// Clone returns an independent snapshot of this registry. Factories are
// shared, while registrations and synchronization are independent.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Registry{factories: maps.Clone(r.factories)}
}

// Register registers a reporter factory by name. Invalid or duplicate
// registrations return errors, rather than panicking.
func (r *Registry) Register(name string, factory Factory) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", ErrInvalidReporterRegistration)
	}
	if factory == nil {
		return fmt.Errorf("%w: nil factory for %q", ErrInvalidReporterRegistration, name)
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

// Unregister removes a factory from this registry only. Built-in "nop" and
// "logging" reporters are protected to keep their property values available.
func (r *Registry) Unregister(name string) {
	if name == ReporterNameNop || name == ReporterNameLogging {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.factories, name)
}

// Names returns the registered reporter names in sorted order.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return slices.Sorted(maps.Keys(r.factories))
}

// FromProperties builds the reporter named by props[ReporterImplKey]. An
// absent or empty name yields [NopReporter] (reporting is opt-in), and an
// unrecognized name is an error. Factory callbacks run outside the registry
// lock.
//
// This nop default intentionally differs from Java, where an absent reporter
// property selects LoggingMetricsReporter. Set ReporterImplKey to
// [ReporterNameLogging] explicitly to opt into logging.
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

// Register registers a factory in the global registry. It returns an error
// for invalid or duplicate registrations. For package init, use MustRegister.
func Register(name string, factory Factory) error {
	return defaultRegistry.Register(name, factory)
}

// MustRegister registers a factory globally or panics, for use in package init.
func MustRegister(name string, factory Factory) {
	if err := Register(name, factory); err != nil {
		panic(err)
	}
}

// Deregister removes a global registration, ignoring unknown and built-in
// names. For independent registrations, use [Registry.Unregister].
func Deregister(name string) {
	defaultRegistry.Unregister(name)
}

// FromProperties builds a reporter from the global factory registry. Use
// [Registry.FromProperties] to resolve names against an isolated registry.
func FromProperties(props map[string]string) (Reporter, error) {
	return defaultRegistry.FromProperties(props)
}

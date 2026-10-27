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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromProperties(t *testing.T) {
	t.Run("absent name yields NopReporter", func(t *testing.T) {
		r, err := FromProperties(nil)
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("empty name yields NopReporter", func(t *testing.T) {
		r, err := FromProperties(map[string]string{ReporterImplKey: ""})
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("nop name yields NopReporter", func(t *testing.T) {
		r, err := FromProperties(map[string]string{ReporterImplKey: "nop"})
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("logging name yields LoggingReporter", func(t *testing.T) {
		r, err := FromProperties(map[string]string{ReporterImplKey: "logging"})
		require.NoError(t, err)
		assert.IsType(t, &LoggingReporter{}, r)
	})

	t.Run("unknown name is an error", func(t *testing.T) {
		_, err := FromProperties(map[string]string{ReporterImplKey: "does-not-exist"})
		assert.Error(t, err)
	})
}

func TestRegister(t *testing.T) {
	t.Run("factory is selectable via FromProperties", func(t *testing.T) {
		Register("test-custom", func(props map[string]string) (Reporter, error) {
			return &InMemoryReporter{}, nil
		})
		t.Cleanup(func() { Deregister("test-custom") })
		r, err := FromProperties(map[string]string{ReporterImplKey: "test-custom"})
		require.NoError(t, err)
		assert.IsType(t, &InMemoryReporter{}, r)
	})

	t.Run("empty name panics", func(t *testing.T) {
		assert.Panics(t, func() { Register("", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }) })
	})

	t.Run("nil factory panics", func(t *testing.T) {
		assert.Panics(t, func() { Register("test-nil-factory", nil) })
	})

	t.Run("duplicate name panics", func(t *testing.T) {
		assert.Panics(t, func() { Register("logging", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }) })
	})
}

func TestDeregisterProtectsBuiltins(t *testing.T) {
	// Deregister must not remove the built-in factories: Register panics on a
	// duplicate, so a removed built-in could never be restored, permanently
	// breaking metrics-reporter-impl=nop/logging for the rest of the process.
	Deregister(ReporterNameNop)
	Deregister(ReporterNameLogging)

	nop, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameNop})
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, nop)

	logging, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
	require.NoError(t, err)
	assert.IsType(t, &LoggingReporter{}, logging)
}

func TestIndependentRegistry(t *testing.T) {
	const name = "test-isolated-reporter"
	props := map[string]string{ReporterImplKey: name}
	one := NewRegistry()
	two := NewRegistry()

	one.Register(name, func(map[string]string) (Reporter, error) {
		return &InMemoryReporter{}, nil
	})

	reporter, err := one.FromProperties(props)
	require.NoError(t, err)
	assert.IsType(t, &InMemoryReporter{}, reporter)

	_, err = two.FromProperties(props)
	require.ErrorContains(t, err, "unknown reporter")
	_, err = FromProperties(props)
	require.ErrorContains(t, err, "unknown reporter")

	one.Deregister(name)
	_, err = one.FromProperties(props)
	require.ErrorContains(t, err, "unknown reporter")
}

func TestRegistryCloneIsASnapshot(t *testing.T) {
	const firstName = "test-registry-first"
	const secondName = "test-registry-second"
	factory := func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	}

	original := NewRegistry()
	original.Register(firstName, factory)
	cloned := original.Clone()

	original.Deregister(firstName)
	_, err := original.FromProperties(map[string]string{ReporterImplKey: firstName})
	require.Error(t, err)

	reporter, err := cloned.FromProperties(map[string]string{ReporterImplKey: firstName})
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, reporter)

	cloned.Register(secondName, factory)
	_, err = original.FromProperties(map[string]string{ReporterImplKey: secondName})
	require.Error(t, err)
}

func TestNewRegistrySnapshotsGlobalRegistrations(t *testing.T) {
	const name = "test-registry-global-snapshot"
	props := map[string]string{ReporterImplKey: name}

	Register(name, func(map[string]string) (Reporter, error) {
		return &InMemoryReporter{}, nil
	})
	t.Cleanup(func() { Deregister(name) })

	snapshot := NewRegistry()
	Deregister(name)

	reporter, err := snapshot.FromProperties(props)
	require.NoError(t, err)
	assert.IsType(t, &InMemoryReporter{}, reporter)

	_, err = FromProperties(props)
	require.ErrorContains(t, err, "unknown reporter")
}

func TestIndependentRegistryProtectsBuiltins(t *testing.T) {
	registry := NewRegistry()
	registry.Deregister(ReporterNameNop)
	registry.Deregister(ReporterNameLogging)

	nop, err := registry.FromProperties(map[string]string{ReporterImplKey: ReporterNameNop})
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, nop)

	logging, err := registry.FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
	require.NoError(t, err)
	assert.IsType(t, &LoggingReporter{}, logging)

	assert.Panics(t, func() { registry.Register(ReporterNameNop, func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	}) })
	assert.Panics(t, func() { registry.Register("", func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	}) })
	assert.Panics(t, func() { registry.Register("test-nil", nil) })
}

func TestRegistryFactoryCanRegisterAnotherFactory(t *testing.T) {
	registry := NewRegistry()
	registry.Register("test-outer", func(map[string]string) (Reporter, error) {
		registry.Register("test-inner", func(map[string]string) (Reporter, error) {
			return NopReporter{}, nil
		})
		return NopReporter{}, nil
	})

	_, err := registry.FromProperties(map[string]string{ReporterImplKey: "test-outer"})
	require.NoError(t, err)
	_, err = registry.FromProperties(map[string]string{ReporterImplKey: "test-inner"})
	require.NoError(t, err)
}

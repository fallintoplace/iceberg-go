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
		require.NoError(t, Register("test-custom", func(props map[string]string) (Reporter, error) {
			return &InMemoryReporter{}, nil
		}))
		t.Cleanup(func() { Deregister("test-custom") })
		r, err := FromProperties(map[string]string{ReporterImplKey: "test-custom"})
		require.NoError(t, err)
		assert.IsType(t, &InMemoryReporter{}, r)
	})

	t.Run("empty name returns an error", func(t *testing.T) {
		err := Register("", func(map[string]string) (Reporter, error) { return NopReporter{}, nil })
		require.ErrorIs(t, err, ErrInvalidReporterRegistration)
	})

	t.Run("nil factory returns an error", func(t *testing.T) {
		err := Register("test-nil-factory", nil)
		require.ErrorIs(t, err, ErrInvalidReporterRegistration)
	})

	t.Run("duplicate name returns an error", func(t *testing.T) {
		err := Register("logging", func(map[string]string) (Reporter, error) { return NopReporter{}, nil })
		require.ErrorIs(t, err, ErrReporterAlreadyRegistered)
	})

	t.Run("MustRegister panics for invalid registrations", func(t *testing.T) {
		assert.Panics(t, func() {
			MustRegister("logging", func(map[string]string) (Reporter, error) { return NopReporter{}, nil })
		})
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

func TestRegistryIsolation(t *testing.T) {
	first := NewRegistry()
	second := NewRegistry()
	factory := func(map[string]string) (Reporter, error) { return &InMemoryReporter{}, nil }
	require.NoError(t, first.Register("test-scoped", factory))

	props := map[string]string{ReporterImplKey: "test-scoped"}
	got, err := first.FromProperties(props)
	require.NoError(t, err)
	assert.IsType(t, &InMemoryReporter{}, got)

	_, err = second.FromProperties(props)
	require.Error(t, err)
	_, err = FromProperties(props)
	require.Error(t, err)

	clone := first.Clone()
	require.NoError(t, clone.Register("test-cloned", factory))
	assert.Equal(t, []string{"logging", "nop", "test-cloned", "test-scoped"}, clone.Names())
	assert.Equal(t, []string{"logging", "nop", "test-scoped"}, first.Names())

	first.Unregister("test-scoped")
	_, err = first.FromProperties(props)
	require.Error(t, err)
	_, err = clone.FromProperties(props)
	require.NoError(t, err)
}

func TestRegistrySnapshotsGlobalRegistrations(t *testing.T) {
	before := NewRegistry()
	factory := func(map[string]string) (Reporter, error) { return NopReporter{}, nil }
	require.NoError(t, Register("test-late-registration", factory))
	t.Cleanup(func() { Deregister("test-late-registration") })

	props := map[string]string{ReporterImplKey: "test-late-registration"}
	_, err := before.FromProperties(props)
	require.Error(t, err)
	_, err = NewRegistry().FromProperties(props)
	require.NoError(t, err)
	_, err = FromProperties(props)
	require.NoError(t, err)
}

func TestRegistryBuiltinsAndValidation(t *testing.T) {
	registry := NewRegistry()
	registry.Unregister(ReporterNameNop)
	registry.Unregister(ReporterNameLogging)
	assert.Empty(t, registry.Names())

	_, err := registry.FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
	require.Error(t, err, "isolated registries may remove a built-in")
	_, err = FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
	require.NoError(t, err, "the global built-in must remain available")

	require.ErrorIs(t, registry.Register("", func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	}), ErrInvalidReporterRegistration)
	require.ErrorIs(t, registry.Register("new", nil), ErrInvalidReporterRegistration)

	registry.MustRegister(ReporterNameNop, func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	})
	require.ErrorIs(t, registry.Register(ReporterNameNop, func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	}), ErrReporterAlreadyRegistered)
	assert.Panics(t, func() {
		registry.MustRegister(ReporterNameNop, func(map[string]string) (Reporter, error) {
			return NopReporter{}, nil
		})
	})

	var empty Registry
	empty.MustRegister("custom", func(map[string]string) (Reporter, error) {
		return NopReporter{}, nil
	})
	assert.Equal(t, []string{"custom"}, empty.Names())
	empty.Unregister("custom")
	assert.Empty(t, empty.Names())
}

func TestRegistryRunsFactoriesWithoutHoldingLock(t *testing.T) {
	registry := NewRegistry()
	require.NoError(t, registry.Register("outer", func(map[string]string) (Reporter, error) {
		err := registry.Register("inner", func(map[string]string) (Reporter, error) {
			return NopReporter{}, nil
		})
		return NopReporter{}, err
	}))

	_, err := registry.FromProperties(map[string]string{ReporterImplKey: "outer"})
	require.NoError(t, err)
	_, err = registry.FromProperties(map[string]string{ReporterImplKey: "inner"})
	require.NoError(t, err)
}

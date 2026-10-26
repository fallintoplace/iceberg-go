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
	"sync"
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
		require.ErrorIs(t, Register("", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }), ErrInvalidReporterRegistration)
	})

	t.Run("nil factory returns an error", func(t *testing.T) {
		require.ErrorIs(t, Register("test-nil-factory", nil), ErrInvalidReporterRegistration)
	})

	t.Run("duplicate name returns an error without replacing the factory", func(t *testing.T) {
		require.ErrorIs(t, Register("logging", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }), ErrReporterAlreadyRegistered)
		got, err := FromProperties(map[string]string{ReporterImplKey: "logging"})
		require.NoError(t, err)
		assert.IsType(t, &LoggingReporter{}, got)
	})
}

func TestDeregisterProtectsBuiltins(t *testing.T) {
	// Deregister must not remove built-ins: an accidental removal could break
	// metrics-reporter-impl=nop/logging for other catalogs.
	Deregister(ReporterNameNop)
	Deregister(ReporterNameLogging)

	nop, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameNop})
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, nop)

	logging, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
	require.NoError(t, err)
	assert.IsType(t, &LoggingReporter{}, logging)
}

func testReporterFactory(map[string]string) (Reporter, error) {
	return &InMemoryReporter{}, nil
}

func TestMustRegisterPanics(t *testing.T) {
	assert.Panics(t, func() { MustRegister("", testReporterFactory) })
	assert.Panics(t, func() { MustRegister("logging", testReporterFactory) })
	assert.Panics(t, func() { MustRegister("invalid", nil) })
}

func TestScopedReporterRegistryIsolation(t *testing.T) {
	t.Parallel()

	first, second := NewRegistry(), NewRegistry()
	require.Equal(t, []string{ReporterNameLogging, ReporterNameNop}, first.Names())
	require.NoError(t, first.Register("private", testReporterFactory))
	require.NoError(t, second.Register("private", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }))

	props := map[string]string{ReporterImplKey: "private"}
	firstReporter, err := first.FromProperties(props)
	require.NoError(t, err)
	assert.IsType(t, &InMemoryReporter{}, firstReporter)
	secondReporter, err := second.FromProperties(props)
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, secondReporter)
	_, err = FromProperties(props)
	require.Error(t, err, "scoped registration must not leak to the global registry")

	clone := first.Clone()
	first.Deregister("private")
	_, err = first.FromProperties(props)
	require.Error(t, err)
	_, err = clone.FromProperties(props)
	require.NoError(t, err)

	names := clone.Names()
	names[0] = "modified"
	assert.Equal(t, []string{ReporterNameLogging, ReporterNameNop, "private"}, clone.Names())
}

func TestScopedReporterRegistryRegistrationErrors(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.ErrorIs(t, reg.Register("", testReporterFactory), ErrInvalidReporterRegistration)
	require.ErrorIs(t, reg.Register("invalid", nil), ErrInvalidReporterRegistration)
	require.NoError(t, reg.Register("chosen", testReporterFactory))
	require.ErrorIs(t, reg.Register("chosen", func(map[string]string) (Reporter, error) { return NopReporter{}, nil }), ErrReporterAlreadyRegistered)
	assert.Equal(t, []string{"chosen", ReporterNameLogging, ReporterNameNop}, reg.Names())

	got, err := reg.FromProperties(map[string]string{ReporterImplKey: "chosen"})
	require.NoError(t, err)
	assert.IsType(t, &InMemoryReporter{}, got)
}

func TestScopedReporterRegistryZeroValue(t *testing.T) {
	t.Parallel()

	var reg Registry
	require.Empty(t, reg.Names())
	require.NoError(t, reg.Register("local", testReporterFactory))
	clone := reg.Clone()
	reg.Deregister("local")

	_, err := clone.FromProperties(map[string]string{ReporterImplKey: "local"})
	require.NoError(t, err)
	_, err = reg.FromProperties(map[string]string{ReporterImplKey: "local"})
	require.Error(t, err)
	got, err := reg.FromProperties(nil)
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, got)
}

func TestScopedReporterRegistrySnapshotsGlobal(t *testing.T) {
	before := NewRegistry()
	require.NoError(t, Register("late-global", testReporterFactory))
	t.Cleanup(func() { Deregister("late-global") })

	props := map[string]string{ReporterImplKey: "late-global"}
	_, err := before.FromProperties(props)
	require.Error(t, err)
	after := NewRegistry()
	_, err = after.FromProperties(props)
	require.NoError(t, err)
	Deregister("late-global")
	_, err = after.FromProperties(props)
	require.NoError(t, err)
}

func TestScopedReporterRegistryAllowsNestedRegistration(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	require.NoError(t, reg.Register("nested", func(map[string]string) (Reporter, error) {
		if err := reg.Register("created", testReporterFactory); err != nil {
			return nil, err
		}
		return NopReporter{}, nil
	}))
	_, err := reg.FromProperties(map[string]string{ReporterImplKey: "nested"})
	require.NoError(t, err)
	_, err = reg.FromProperties(map[string]string{ReporterImplKey: "created"})
	require.NoError(t, err)
}

func TestScopedReporterRegistryPropagatesFactoryError(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	want := errors.New("factory failed")
	require.NoError(t, reg.Register("failure", func(map[string]string) (Reporter, error) { return nil, want }))
	_, err := reg.FromProperties(map[string]string{ReporterImplKey: "failure"})
	require.ErrorIs(t, err, want)
}

func TestScopedReporterRegistryConcurrentUse(t *testing.T) {
	t.Parallel()

	reg := NewRegistry()
	const workers = 24
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			name := fmt.Sprintf("worker-%d", id)
			if err := reg.Register(name, testReporterFactory); err != nil {
				errs <- err
				return
			}
			if _, err := reg.FromProperties(map[string]string{ReporterImplKey: name}); err != nil {
				errs <- err
			}
			_ = reg.Names()
			reg.Deregister(name)
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

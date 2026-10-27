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

	t.Run("empty name returns error", func(t *testing.T) {
		err := Register("", func(map[string]string) (Reporter, error) { return NopReporter{}, nil })
		require.ErrorContains(t, err, "empty name")
	})

	t.Run("nil factory returns error", func(t *testing.T) {
		err := Register("test-nil-factory", nil)
		require.ErrorContains(t, err, "nil factory")
		_, err = FromProperties(map[string]string{ReporterImplKey: "test-nil-factory"})
		assert.ErrorContains(t, err, "unknown reporter")
	})

	t.Run("duplicate name returns error without replacing factory", func(t *testing.T) {
		err := Register(ReporterNameLogging, func(map[string]string) (Reporter, error) {
			return NopReporter{}, nil
		})
		require.ErrorContains(t, err, "already registered")

		r, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
		require.NoError(t, err)
		assert.IsType(t, &LoggingReporter{}, r)
	})
}

func TestMustRegister(t *testing.T) {
	t.Run("registers a valid factory", func(t *testing.T) {
		MustRegister("test-must-register", func(map[string]string) (Reporter, error) {
			return NopReporter{}, nil
		})
		t.Cleanup(func() { Deregister("test-must-register") })

		r, err := FromProperties(map[string]string{ReporterImplKey: "test-must-register"})
		require.NoError(t, err)
		assert.IsType(t, NopReporter{}, r)
	})

	t.Run("panics on empty name", func(t *testing.T) {
		assert.Panics(t, func() {
			MustRegister("", func(map[string]string) (Reporter, error) { return NopReporter{}, nil })
		})
	})

	t.Run("panics on nil factory", func(t *testing.T) {
		assert.Panics(t, func() { MustRegister("test-must-nil-factory", nil) })
	})

	t.Run("panics on duplicate name", func(t *testing.T) {
		assert.Panics(t, func() {
			MustRegister(ReporterNameLogging, func(map[string]string) (Reporter, error) {
				return NopReporter{}, nil
			})
		})
	})
}

func TestDeregisterProtectsBuiltins(t *testing.T) {
	// Deregister must not remove built-in factories: removing one would
	// break metrics-reporter-impl=nop/logging for all later callers.
	Deregister(ReporterNameNop)
	Deregister(ReporterNameLogging)

	nop, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameNop})
	require.NoError(t, err)
	assert.IsType(t, NopReporter{}, nop)

	logging, err := FromProperties(map[string]string{ReporterImplKey: ReporterNameLogging})
	require.NoError(t, err)
	assert.IsType(t, &LoggingReporter{}, logging)
}

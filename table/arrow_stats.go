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

package table

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

type arrowStatsCollector struct {
	fieldID          int
	schema           *iceberg.Schema
	defaultMode      tblutils.MetricsMode
	defaultModeError error
	columnModes      map[string]tblutils.MetricsMode
	columnModeErrors map[string]error
}

func (a *arrowStatsCollector) resolveColumnMetricsMode(colName string) tblutils.MetricsMode {
	if a.defaultModeError != nil {
		panic(a.defaultModeError)
	}

	if metMode, ok := a.columnModes[colName]; ok {
		return metMode
	}
	if err, ok := a.columnModeErrors[colName]; ok {
		panic(err)
	}

	return a.defaultMode
}

func (a *arrowStatsCollector) primitiveCollector(dt iceberg.PrimitiveType, isNested bool) (tblutils.StatisticsCollector, bool) {
	colName, ok := a.schema.FindColumnName(a.fieldID)
	if !ok {
		return tblutils.StatisticsCollector{}, false
	}

	metMode := a.resolveColumnMetricsMode(colName)

	switch dt.(type) {
	case iceberg.StringType:
	case iceberg.BinaryType:
	default:
		if metMode.Typ == tblutils.MetricModeTruncate {
			metMode = tblutils.MetricsMode{Typ: tblutils.MetricModeFull, Len: 0}
		}
	}

	if isNested && (metMode.Typ == tblutils.MetricModeTruncate || metMode.Typ == tblutils.MetricModeFull) {
		metMode = tblutils.MetricsMode{Typ: tblutils.MetricModeCounts}
	}

	return tblutils.StatisticsCollector{
		FieldID:    a.fieldID,
		IcebergTyp: dt,
		ColName:    colName,
		Mode:       metMode,
	}, true
}

func (a *arrowStatsCollector) variantCollector() (tblutils.StatisticsCollector, bool) {
	colName, ok := a.schema.FindColumnName(a.fieldID)
	if !ok {
		return tblutils.StatisticsCollector{}, false
	}

	return tblutils.StatisticsCollector{
		FieldID: a.fieldID,
		ColName: colName,
		Mode:    a.resolveColumnMetricsMode(colName),
	}, true
}

func statsPlanFieldCount(field iceberg.NestedField) int {
	switch typ := field.Type.(type) {
	case *iceberg.StructType:
		count := 0
		for _, nestedField := range typ.FieldList {
			count += statsPlanFieldCount(nestedField)
		}

		return count
	case *iceberg.ListType:
		return statsPlanFieldCount(typ.ElementField())
	case *iceberg.MapType:
		return statsPlanFieldCount(typ.KeyField()) + statsPlanFieldCount(typ.ValueField())
	default:
		return 1
	}
}

func collectStatsPlanField(visitor *arrowStatsCollector, result map[int]tblutils.StatisticsCollector, field iceberg.NestedField, isNested bool) {
	switch typ := field.Type.(type) {
	case *iceberg.StructType:
		for _, nestedField := range typ.FieldList {
			collectStatsPlanField(visitor, result, nestedField, true)
		}
	case *iceberg.ListType:
		collectStatsPlanField(visitor, result, typ.ElementField(), true)
	case *iceberg.MapType:
		collectStatsPlanField(visitor, result, typ.KeyField(), true)
		collectStatsPlanField(visitor, result, typ.ValueField(), true)
	case iceberg.VariantType:
		visitor.fieldID = field.ID
		if collector, ok := visitor.variantCollector(); ok {
			result[collector.FieldID] = collector
		}
	default:
		visitor.fieldID = field.ID
		collector, ok := visitor.primitiveCollector(field.Type.(iceberg.PrimitiveType), isNested)
		if ok {
			result[collector.FieldID] = collector
		}
	}
}

func computeStatsPlan(sc *iceberg.Schema, props iceberg.Properties) (result map[int]tblutils.StatisticsCollector, err error) {
	defer func() {
		if r := recover(); r != nil {
			result = nil
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("%w: %s", iceberg.ErrInvalidSchema, e)
			case error:
				err = fmt.Errorf("error encountered during schema visitor: %w", e)
			}
		}
	}()

	if sc == nil {
		return nil, fmt.Errorf("%w: cannot visit nil schema", iceberg.ErrInvalidArgument)
	}

	defaultMode, defaultModeErr := tblutils.MatchMetricsMode(
		props.Get(DefaultWriteMetricsModeKey, DefaultWriteMetricsModeDefault))
	overrideCount := 0
	for key := range props {
		if strings.HasPrefix(key, MetricsModeColumnConfPrefix+".") {
			overrideCount++
		}
	}

	var columnModes map[string]tblutils.MetricsMode
	var columnModeErrors map[string]error
	for key, rawMode := range props {
		colName, ok := strings.CutPrefix(key, MetricsModeColumnConfPrefix+".")
		if !ok {
			continue
		}

		mode, err := tblutils.MatchMetricsMode(rawMode)
		if err != nil {
			if columnModeErrors == nil {
				columnModeErrors = make(map[string]error, overrideCount)
			}

			columnModeErrors[colName] = err

			continue
		}
		if columnModes == nil {
			columnModes = make(map[string]tblutils.MetricsMode, overrideCount)
		}
		columnModes[colName] = mode
	}

	visitor := &arrowStatsCollector{
		schema:           sc,
		defaultMode:      defaultMode,
		defaultModeError: defaultModeErr,
		columnModes:      columnModes,
		columnModeErrors: columnModeErrors,
	}

	fields := sc.FieldsRef(internal.SchemaRef{})
	resultCount := 0
	for _, field := range fields {
		resultCount += statsPlanFieldCount(field)
	}

	result = make(map[int]tblutils.StatisticsCollector, resultCount)
	for _, field := range fields {
		collectStatsPlanField(visitor, result, field, strings.Contains(field.Name, "."))
	}

	return result, nil
}

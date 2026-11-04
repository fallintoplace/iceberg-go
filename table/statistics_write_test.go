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
	"slices"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

func TestTransactionPuffinStatisticsRegistration(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "puffin"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	stats := StatisticsFile{
		SnapshotID:            101,
		StatisticsPath:        "s3://warehouse/db/puffin/metadata/first.puffin",
		FileSizeInBytes:       512,
		FileFooterSizeInBytes: 64,
		BlobMetadata:          []BlobMetadata{},
	}

	require.NoError(t, tx.SetStatistics(stats))
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats}, slices.Collect(meta.Statistics()))
	require.Equal(t, int64(103), meta.CurrentSnapshot().SnapshotID)

	second := stats
	second.SnapshotID = 103
	second.StatisticsPath = "s3://warehouse/db/puffin/metadata/second.puffin"
	require.NoError(t, tx.SetStatistics(second))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{stats, second}, slices.Collect(meta.Statistics()))

	replacement := stats
	replacement.StatisticsPath = "s3://warehouse/db/puffin/metadata/replacement.puffin"
	require.NoError(t, tx.SetStatistics(replacement))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{replacement, second}, slices.Collect(meta.Statistics()))

	require.NoError(t, tx.RemoveStatistics(stats.SnapshotID))
	require.NoError(t, tx.RemoveStatistics(stats.SnapshotID))
	meta, err = tx.meta.Build()
	require.NoError(t, err)
	require.Equal(t, []StatisticsFile{second}, slices.Collect(meta.Statistics()))
	require.Equal(t, int64(103), meta.CurrentSnapshot().SnapshotID)
}

func TestTransactionPuffinStatisticsValidation(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "puffin"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	stats := StatisticsFile{
		SnapshotID:            103,
		StatisticsPath:        "s3://warehouse/db/puffin/metadata/stats.puffin",
		FileSizeInBytes:       100,
		FileFooterSizeInBytes: 42,
		BlobMetadata:          []BlobMetadata{},
	}

	for _, tc := range []struct {
		name string
		edit func(*StatisticsFile)
	}{
		{"empty path", func(s *StatisticsFile) { s.StatisticsPath = "" }},
		{"empty file", func(s *StatisticsFile) { s.FileSizeInBytes = 0 }},
		{"negative file size", func(s *StatisticsFile) { s.FileSizeInBytes = -1 }},
		{"empty footer", func(s *StatisticsFile) { s.FileFooterSizeInBytes = 0 }},
		{"negative footer", func(s *StatisticsFile) { s.FileFooterSizeInBytes = -1 }},
		{"footer exceeds file", func(s *StatisticsFile) { s.FileFooterSizeInBytes = 101 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := stats
			tc.edit(&bad)
			tx := tbl.NewTransaction()
			require.ErrorIs(t, tx.SetStatistics(bad), iceberg.ErrInvalidArgument)
			require.Empty(t, tx.meta.updates)
		})
	}

	tx := tbl.NewTransaction()
	stats.SnapshotID = 999
	require.ErrorIs(t, tx.SetStatistics(stats), ErrSnapshotNotFound)
	require.Empty(t, tx.meta.updates)

	var nilTx *Transaction
	require.Error(t, nilTx.SetStatistics(stats))
	require.Error(t, nilTx.RemoveStatistics(103))
}

func TestTransactionPuffinStatisticsClonesMetadata(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "puffin"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	key := "original-key"
	stats := StatisticsFile{
		SnapshotID:            103,
		StatisticsPath:        "s3://warehouse/db/puffin/metadata/stats.puffin",
		FileSizeInBytes:       100,
		FileFooterSizeInBytes: 42,
		KeyMetadata:           &key,
		BlobMetadata: []BlobMetadata{{
			Type:           "custom-stats",
			SnapshotID:     103,
			SequenceNumber: 2,
			Fields:         []int32{1, 2},
			Properties:     map[string]string{"source": "spark"},
		}},
	}
	require.NoError(t, tx.SetStatistics(stats))

	key = "changed-key"
	stats.BlobMetadata[0].Fields[0] = 99
	stats.BlobMetadata[0].Properties["source"] = "changed"
	stats.BlobMetadata[0].Type = "different"

	meta, err := tx.meta.Build()
	require.NoError(t, err)
	registered := slices.Collect(meta.Statistics())
	require.Len(t, registered, 1)
	require.Equal(t, "original-key", *registered[0].KeyMetadata)
	require.Equal(t, []int32{1, 2}, registered[0].BlobMetadata[0].Fields)
	require.Equal(t, "spark", registered[0].BlobMetadata[0].Properties["source"])
	require.EqualValues(t, "custom-stats", registered[0].BlobMetadata[0].Type)

	updates := tx.meta.updates
	require.Len(t, updates, 1)
	set, ok := updates[0].(*setStatisticsUpdate)
	require.True(t, ok)
	require.Equal(t, "spark", set.Statistics.BlobMetadata[0].Properties["source"])
}

func TestTransactionPuffinStatisticsEmptyBlobMetadata(t *testing.T) {
	tbl := inspectTestTable(Identifier{"db", "puffin"}, map[string]SnapshotRef{
		MainBranch: {SnapshotID: 103, Type: BranchRef},
	})
	tx := tbl.NewTransaction()
	require.NoError(t, tx.SetStatistics(StatisticsFile{
		SnapshotID: 103, StatisticsPath: "s3://warehouse/stats.puffin",
		FileSizeInBytes: 100, FileFooterSizeInBytes: 42,
	}))
	meta, err := tx.meta.Build()
	require.NoError(t, err)
	stats := slices.Collect(meta.Statistics())
	require.Len(t, stats, 1)
	require.NotNil(t, stats[0].BlobMetadata)
	require.Empty(t, stats[0].BlobMetadata)
}

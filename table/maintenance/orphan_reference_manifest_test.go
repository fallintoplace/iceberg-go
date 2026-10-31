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

package maintenance

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func referenceManifestFixture(t testing.TB, n int) (*table.Table, *trackingIO) {
	t.Helper()
	fs := newTrackingIO()
	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "x", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	spec := iceberg.NewPartitionSpec()
	snapshotID := int64(1)
	entries := make([]iceberg.ManifestEntry, n)
	for i := range entries {
		df, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, fmt.Sprintf("s3://bucket/data/%d.parquet", i), iceberg.ParquetFile, nil, nil, nil, 1, 1024)
		require.NoError(t, err)
		entries[i] = iceberg.NewManifestEntryBuilder(iceberg.EntryStatusADDED, &snapshotID, df.Build()).DataSequenceNumber(1).Build()
	}
	manifestPath := "s3://bucket/meta/manifest.avro"
	listPath := "s3://bucket/meta/snap.avro"
	var buf bytes.Buffer
	_, err := iceberg.WriteManifest(manifestPath, &buf, 2, spec, schema, snapshotID, entries)
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(context.Background(), manifestPath, buf.Bytes()))
	mf := iceberg.NewManifestBuilder(2, manifestPath, int64(buf.Len()), 0, snapshotID).SequenceNumber(1, 1).AddedFiles(int32(n)).AddedRows(int64(n)).Build()
	writeManifestList(t, fs, snapshotID, listPath, []iceberg.ManifestFile{mf})
	meta, err := table.ParseMetadataString(buildMetaJSON(metaJSONOpts{snapshots: fmt.Sprintf(`{"snapshot-id":1,"timestamp-ms":1000,"manifest-list":%q}`, listPath)}))
	require.NoError(t, err)

	return table.New(table.Identifier{"ns", "tbl"}, meta, "metadata.json", testFSF(fs), nil), fs
}

func TestGetReferencedFilesAcrossBatchBoundary(t *testing.T) {
	tbl, fs := referenceManifestFixture(t, 1025)
	refs, err := getReferencedFiles(context.Background(), tbl, fs, 2, true)
	require.NoError(t, err)
	for i := range 1025 {
		require.True(t, refs[fmt.Sprintf("s3://bucket/data/%d.parquet", i)])
	}
}

func BenchmarkGetReferencedFilesManifestEntries(b *testing.B) {
	for _, n := range []int{1, 16384} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			tbl, fs := referenceManifestFixture(b, n)
			b.ReportAllocs()
			for b.Loop() {
				refs, err := getReferencedFiles(context.Background(), tbl, fs, 1, true)
				if err != nil || len(refs) != n+4 {
					b.Fatalf("references=%d error=%v", len(refs), err)
				}
			}
		})
	}
}

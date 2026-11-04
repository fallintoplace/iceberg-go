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

	"github.com/apache/iceberg-go"
)

// SetStatistics registers an existing Puffin statistics file for its snapshot.
// It replaces an earlier statistics file for that snapshot, if present. The
// caller must have written a valid Puffin file before registering it; this
// method only stages the metadata reference and does not write the file.
// Commit publishes the reference without creating a new table snapshot.
func (t *Transaction) SetStatistics(stats StatisticsFile) error {
	meta, err := t.txnMeta()
	if err != nil {
		return err
	}

	if stats.StatisticsPath == "" || stats.FileSizeInBytes <= 0 ||
		stats.FileFooterSizeInBytes <= 0 || stats.FileFooterSizeInBytes > stats.FileSizeInBytes {
		return fmt.Errorf("%w: statistics require a nonempty path and positive file and footer sizes, with footer size not exceeding file size",
			iceberg.ErrInvalidArgument)
	}
	if _, err := meta.SnapshotByID(stats.SnapshotID); err != nil {
		return fmt.Errorf("set statistics: %w", err)
	}

	// The spec requires blob-metadata to be a list, including when empty.
	if stats.BlobMetadata == nil {
		stats.BlobMetadata = []BlobMetadata{}
	}

	// Don't let caller mutations change the staged update or the eventual
	// catalog request. Blob fields and properties contain mutable slices/maps.
	stats = cloneStatisticsFiles([]StatisticsFile{stats})[0]

	return t.apply([]Update{NewSetStatisticsUpdate(stats)}, nil)
}

// RemoveStatistics removes the metadata reference to a Puffin statistics file
// for snapshotID. It does not delete the physical file. Removing an absent
// reference succeeds, matching Iceberg Java's updateStatistics behavior.
func (t *Transaction) RemoveStatistics(snapshotID int64) error {
	if _, err := t.txnMeta(); err != nil {
		return err
	}

	return t.apply([]Update{NewRemoveStatisticsUpdate(snapshotID)}, nil)
}

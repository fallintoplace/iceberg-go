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

package iceberg

import (
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"

	"github.com/apache/iceberg-go/internal"
	"github.com/twmb/avro/ocf"
)

type ManifestListWriter struct {
	version          int
	out              io.Writer
	commitSnapshotID int64
	sequenceNumber   int64
	writer           *ocf.Writer
	nextRowID        *int64
	failure          error
}

func NewManifestListWriterV1(out io.Writer, snapshotID int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          1,
		out:              out,
		commitSnapshotID: snapshotID,
		sequenceNumber:   -1,
	}

	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.FormatInt(*parentSnapshot, 10)
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.FormatInt(snapshotID, 10)),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func NewManifestListWriterV2(out io.Writer, snapshotID, sequenceNumber int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	m := &ManifestListWriter{
		version:          2,
		out:              out,
		commitSnapshotID: snapshotID,
		sequenceNumber:   sequenceNumber,
	}

	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.FormatInt(*parentSnapshot, 10)
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.FormatInt(snapshotID, 10)),
		"sequence-number":    []byte(strconv.FormatInt(sequenceNumber, 10)),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func NewManifestListWriterV3(out io.Writer, snapshotId, sequenceNumber, firstRowID int64, parentSnapshot *int64) (*ManifestListWriter, error) {
	if firstRowID < 0 {
		return nil, fmt.Errorf("%w: first row ID must be non-negative: %d", ErrInvalidArgument, firstRowID)
	}

	m := &ManifestListWriter{
		version:          3,
		out:              out,
		commitSnapshotID: snapshotId,
		sequenceNumber:   sequenceNumber,
		nextRowID:        &firstRowID,
	}
	parentSnapshotStr := "null"
	if parentSnapshot != nil {
		parentSnapshotStr = strconv.FormatInt(*parentSnapshot, 10)
	}

	return m, m.init(map[string][]byte{
		"format-version":     []byte(strconv.Itoa(m.version)),
		"snapshot-id":        []byte(strconv.FormatInt(snapshotId, 10)),
		"sequence-number":    []byte(strconv.FormatInt(sequenceNumber, 10)),
		"first-row-id":       []byte(strconv.FormatInt(firstRowID, 10)),
		"parent-snapshot-id": []byte(parentSnapshotStr),
	})
}

func advanceRowID(firstRowID, existingRows, addedRows int64) (int64, error) {
	if firstRowID < 0 {
		return 0, fmt.Errorf("%w: first row ID must be non-negative: %d", ErrInvalidArgument, firstRowID)
	}
	if existingRows == -1 || addedRows == -1 {
		return 0, fmt.Errorf("%w: cannot assign row-lineage IDs because at least one row count is unknown (existing=%d added=%d); rewrite or compact the legacy manifest before upgrading to v3",
			ErrInvalidArgument, existingRows, addedRows)
	}
	if existingRows < 0 || addedRows < 0 {
		return 0, fmt.Errorf("%w: row counts must be non-negative: existing=%d added=%d",
			ErrInvalidArgument, existingRows, addedRows)
	}
	if existingRows > math.MaxInt64-firstRowID || addedRows > math.MaxInt64-firstRowID-existingRows {
		return 0, fmt.Errorf("%w: assigning %d existing and %d added rows from first row ID %d overflows int64",
			ErrInvalidArgument, existingRows, addedRows, firstRowID)
	}

	return firstRowID + existingRows + addedRows, nil
}

func (m *ManifestListWriter) init(meta map[string][]byte) error {
	fileSchema, err := internal.NewManifestFileSchema(m.version)
	if err != nil {
		return err
	}

	wr, err := ocf.NewWriter(m.out, fileSchema,
		ocf.WithSchema(fileSchema.String()),
		ocf.WithMetadata(meta),
		ocf.WithCodec(ocf.DeflateCodec(-1)))
	if err != nil {
		return err
	}

	m.writer = wr

	return nil
}

func (m *ManifestListWriter) Close() error {
	if m.writer == nil {
		return nil
	}

	return m.writer.Close()
}

func (m *ManifestListWriter) NextRowID() *int64 {
	return m.nextRowID
}

// AddManifests appends manifest files to the list. If it returns an error, the
// writer is poisoned: callers must close and discard it, and subsequent calls
// return the original error.
func (m *ManifestListWriter) AddManifests(files []ManifestFile) (err error) {
	if m.failure != nil {
		return m.failure
	}
	if len(files) == 0 {
		return nil
	}
	hasRowID := m.version == 3 && m.nextRowID != nil
	var batchStartRowID int64
	if hasRowID {
		batchStartRowID = *m.nextRowID
	}
	defer func() {
		if err != nil {
			if hasRowID {
				*m.nextRowID = batchStartRowID
			}
			m.failure = err
		}
	}()

	switch m.version {
	case 1:
		if slices.ContainsFunc(files, func(f ManifestFile) bool {
			return f.Version() != 1
		}) {
			return fmt.Errorf("%w: ManifestListWriter only supports version 1 manifest files", ErrInvalidArgument)
		}

		var tmp manifestFileV1
		for _, file := range files {
			file.(*manifestFile).toV1(&tmp)
			ensurePartitionList(&tmp.PartitionList)
			if err := m.writer.Encode(&tmp); err != nil {
				return err
			}
		}

	case 2, 3:
		for _, file := range files {
			var assignedNextRowID *int64

			// Per the Iceberg spec a v2 manifest list may reference v1 manifest
			// files (and a v3 list may reference v1 or v2 manifests) so that a
			// table can be upgraded without rewriting historical manifests. The
			// in-memory ManifestFile produced for v1 inputs already carries the
			// inheritance values mandated by the spec — Content=data and
			// SeqNumber/MinSeqNumber=0 — so it can be encoded directly against
			// the v2/v3 entry schema. Newer-than-writer inputs are rejected
			// because the v2 schema cannot represent v3 fields such as
			// first_row_id.
			if file.Version() > m.version {
				return fmt.Errorf("%w: manifest list v%d cannot reference v%d manifest files",
					ErrInvalidArgument, m.version, file.Version())
			}

			wrapped := *file.(*manifestFile)
			ensurePartitionList(&wrapped.PartitionList)
			if m.version == 3 {
				// Ref: https://github.com/apache/iceberg/blob/ea2071568dc66148b483a82eefedcd2992b435f7/core/src/main/java/org/apache/iceberg/ManifestListWriter.java#L157-L168
				if wrapped.Content == ManifestContentData {
					if wrapped.FirstRowIDValue == nil {
						if m.nextRowID != nil {
							firstRowID := *m.nextRowID
							nextRowID, err := advanceRowID(firstRowID, wrapped.ExistingRowsCount, wrapped.AddedRowsCount)
							if err != nil {
								return fmt.Errorf("manifest %q: %w", wrapped.Path, err)
							}
							wrapped.FirstRowIDValue = &firstRowID
							assignedNextRowID = &nextRowID
						}
					}
				}
			}
			if wrapped.SeqNumber == -1 {
				// if the sequence number is being assigned here,
				// then the manifest must be created by the current
				// operation.
				// to validate this, check the snapshot id matches the current commit
				if m.commitSnapshotID != wrapped.AddedSnapshotID {
					return fmt.Errorf("found unassigned sequence number for a manifest from snapshot %d != %d",
						m.commitSnapshotID, wrapped.AddedSnapshotID)
				}
				wrapped.SeqNumber = m.sequenceNumber
			}

			if wrapped.MinSeqNumber == -1 {
				if m.commitSnapshotID != wrapped.AddedSnapshotID {
					return fmt.Errorf("found unassigned sequence number for a manifest from snapshot: %d", wrapped.AddedSnapshotID)
				}
				// if the min sequence number is not determined, then there was no assigned sequence number
				// for any file written to the wrapped manifest. replace the unassigned sequence number with
				// the one for this commit
				wrapped.MinSeqNumber = m.sequenceNumber
			}
			if err := m.writer.Encode(wrapped); err != nil {
				return err
			}
			if assignedNextRowID != nil {
				*m.nextRowID = *assignedNextRowID
			}
		}
	default:
		return fmt.Errorf("unsupported manifest version: %d", m.version)
	}

	return nil
}

// WriteManifestList writes a list of manifest files to an avro file.
func WriteManifestList(version int, out io.Writer, snapshotID int64, parentSnapshotID, sequenceNumber *int64, firstRowId int64, files []ManifestFile) (err error) {
	var writer *ManifestListWriter

	switch version {
	case 1:
		writer, err = NewManifestListWriterV1(out, snapshotID, parentSnapshotID)
	case 2:
		if sequenceNumber == nil {
			return errors.New("sequence number is required for V2 tables")
		}
		writer, err = NewManifestListWriterV2(out, snapshotID, *sequenceNumber, parentSnapshotID)
	case 3:
		if sequenceNumber == nil {
			return errors.New("sequence number is required for V3 tables")
		}
		writer, err = NewManifestListWriterV3(out, snapshotID, *sequenceNumber, firstRowId, parentSnapshotID)
	default:
		return fmt.Errorf("unsupported manifest version: %d", version)
	}

	if err != nil {
		return err
	}
	defer internal.CheckedClose(writer, &err)

	return writer.AddManifests(files)
}

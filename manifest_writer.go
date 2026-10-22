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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"

	"github.com/apache/iceberg-go/internal"
	"github.com/twmb/avro/ocf"
)

func validateManifestFormatVersion(version int) error {
	if version < 1 || version > 3 {
		return fmt.Errorf("unsupported manifest format version: %d", version)
	}

	return nil
}

type writerImpl interface {
	prepareEntry(*manifestEntry, int64) (ManifestEntry, error)
}

type v1writerImpl struct{}

func (v1writerImpl) prepareEntry(entry *manifestEntry, sn int64) (ManifestEntry, error) {
	if entry.Snapshot != nil && *entry.Snapshot != sn {
		if entry.EntryStatus != EntryStatusEXISTING {
			return nil, fmt.Errorf("mismatched snapshot id for entry: %d vs %d", *entry.Snapshot, sn)
		}
		sn = *entry.Snapshot
	}

	return &fallbackManifestEntry{
		manifestEntry: *entry,
		Snapshot:      sn,
	}, nil
}

type v2writerImpl struct{}

func (v2writerImpl) prepareEntry(entry *manifestEntry, snapshotID int64) (ManifestEntry, error) {
	if entry.SeqNum == nil {
		if entry.Snapshot != nil && *entry.Snapshot != snapshotID {
			return nil, fmt.Errorf("found unassigned sequence number for entry from snapshot: %d", entry.SnapshotID())
		}

		if entry.EntryStatus != EntryStatusADDED {
			return nil, errors.New("only entries with status ADDED can be missing a sequence number")
		}
	}

	return entry, nil
}

type v3writerImpl struct{}

func (v3writerImpl) prepareEntry(entry *manifestEntry, snapshotID int64) (ManifestEntry, error) {
	if entry.SeqNum == nil {
		if entry.Snapshot != nil && *entry.Snapshot != snapshotID {
			return nil, fmt.Errorf("found unassigned sequence number for entry from snapshot: %d", entry.SnapshotID())
		}
		if entry.EntryStatus != EntryStatusADDED {
			return nil, errors.New("only entries with status ADDED can be missing a sequence number")
		}
	}

	return entry, nil
}

type ManifestWriter struct {
	closed   bool
	closeErr error
	version  int
	impl     writerImpl

	output io.Writer
	writer *ocf.Writer

	spec    PartitionSpec
	schema  *Schema
	content ManifestContent

	partFieldMaps dataFileFieldMaps

	snapshotID    int64
	addedFiles    int32
	addedRows     int64
	existingFiles int32
	existingRows  int64
	deletedFiles  int32
	deletedRows   int64

	partitionStats    *partitionSummaryStats
	partitionStatsErr error
	minSeqNum         int64
	reusedEntry       manifestEntry
}

type ManifestWriterOption func(w *ManifestWriter)

func WithManifestWriterContent(content ManifestContent) ManifestWriterOption {
	return func(w *ManifestWriter) {
		w.content = content
	}
}

func NewManifestWriter(version int, out io.Writer, spec PartitionSpec, schema *Schema, snapshotID int64, opts ...ManifestWriterOption) (*ManifestWriter, error) {
	var impl writerImpl

	switch version {
	case 1:
		impl = v1writerImpl{}
	case 2:
		impl = v2writerImpl{}
	case 3:
		impl = v3writerImpl{}
	default:
		return nil, fmt.Errorf("unsupported manifest version: %d", version)
	}

	sc, err := partitionTypeToAvroSchema(spec.PartitionType(schema))
	if err != nil {
		return nil, err
	}

	fileSchema, err := internal.NewManifestEntrySchema(sc, version)
	if err != nil {
		return nil, err
	}

	fieldMaps := getFieldIDMap(fileSchema)
	partitionStats, partitionStatsErr := newPartitionSummaryStats(spec, schema)

	w := &ManifestWriter{
		impl:              impl,
		version:           version,
		output:            out,
		spec:              spec,
		content:           ManifestContentData,
		schema:            schema,
		partFieldMaps:     fieldMaps,
		snapshotID:        snapshotID,
		partitionStats:    partitionStats,
		partitionStatsErr: partitionStatsErr,
		minSeqNum:         -1,
	}

	for _, apply := range opts {
		apply(w)
	}
	if w.content != ManifestContentData && w.content != ManifestContentDeletes {
		return nil, fmt.Errorf("%w: invalid manifest content: %d", ErrInvalidArgument, w.content)
	}
	if version < 2 && w.content != ManifestContentData {
		return nil, fmt.Errorf("unsupported content '%s' for format version '%d'", w.content, version)
	}

	md, err := w.meta()
	if err != nil {
		return nil, err
	}

	wr, err := ocf.NewWriter(out, fileSchema,
		ocf.WithSchema(fileSchema.String()),
		ocf.WithMetadata(md),
		ocf.WithCodec(ocf.DeflateCodec(-1)))

	w.writer = wr

	return w, err
}

func (w *ManifestWriter) Close() error {
	if w.closed {
		return w.closeErr
	}
	w.closed = true

	var emptyErr error
	if w.addedFiles+w.existingFiles+w.deletedFiles == 0 {
		emptyErr = ErrEmptyManifest
	}

	var writerErr error
	if w.writer != nil {
		writerErr = w.writer.Close()
	}

	switch {
	case emptyErr == nil:
		w.closeErr = writerErr
	case writerErr == nil:
		w.closeErr = emptyErr
	default:
		w.closeErr = errors.Join(emptyErr, writerErr)
	}

	return w.closeErr
}

type ManifestFileOption func(mf *manifestFile)

// WithManifestFileContent sets the ManifestContent of a new manifest file.
// The default is the content the ManifestWriter was created with, so this is
// only needed to state that content explicitly. Passing a value that disagrees
// with the writer makes ToManifestFile fail.
func WithManifestFileContent(content ManifestContent) ManifestFileOption {
	return func(mf *manifestFile) {
		mf.Content = content
	}
}

func (w *ManifestWriter) ToManifestFile(location string, length int64, opts ...ManifestFileOption) (ManifestFile, error) {
	if err := w.Close(); err != nil {
		return nil, err
	}

	if w.partitionStatsErr != nil {
		return nil, w.partitionStatsErr
	}
	partitions := w.partitionStats.summaries()

	mf := manifestFile{
		version:            w.version,
		Path:               location,
		Len:                length,
		SpecID:             int32(w.spec.id),
		Content:            w.content,
		SeqNumber:          -1,
		MinSeqNumber:       w.minSeqNum,
		AddedSnapshotID:    w.snapshotID,
		AddedFilesCount:    w.addedFiles,
		ExistingFilesCount: w.existingFiles,
		DeletedFilesCount:  w.deletedFiles,
		AddedRowsCount:     w.addedRows,
		ExistingRowsCount:  w.existingRows,
		DeletedRowsCount:   w.deletedRows,
		PartitionList:      &partitions,
		Key:                nil,
	}
	for _, apply := range opts {
		apply(&mf)
	}
	// The writer already stamped w.content into the manifest's own avro metadata,
	// so a different content here would contradict the file it describes.
	if mf.Content != w.content {
		return nil, fmt.Errorf("%w: manifest file content '%s' does not match writer content '%s'",
			ErrInvalidArgument, mf.Content, w.content)
	}

	if mf.Content == ManifestContentDeletes && mf.FirstRowIDValue != nil {
		return nil, errors.New("first_row_id must not be set on delete manifests")
	}

	return &mf, nil
}

func (w *ManifestWriter) meta() (map[string][]byte, error) {
	schemaJson, err := json.Marshal(w.schema)
	if err != nil {
		return nil, err
	}

	specFields := w.spec.fields

	if specFields == nil {
		specFields = []PartitionField{}
	}

	specFieldsJson, err := json.Marshal(specFields)
	if err != nil {
		return nil, err
	}

	return map[string][]byte{
		"schema":            schemaJson,
		"schema-id":         []byte(strconv.Itoa(w.schema.ID)),
		"partition-spec":    specFieldsJson,
		"partition-spec-id": []byte(strconv.Itoa(w.spec.ID())),
		"format-version":    []byte(strconv.Itoa(w.version)),
		"content":           []byte(w.content.String()),
	}, nil
}

func (w *ManifestWriter) addEntry(entry *manifestEntry) error {
	if w.closed {
		return errors.New("cannot add entry to closed manifest writer")
	}

	status := entry.Status()
	switch status {
	case EntryStatusADDED, EntryStatusEXISTING, EntryStatusDELETED:
	default:
		return fmt.Errorf("unknown entry status: %v", status)
	}
	if w.version > 1 {
		entryContent := entry.DataFile().ContentType()
		switch entryContent {
		case EntryContentData:
			if w.content != ManifestContentData {
				return fmt.Errorf("%w: data file cannot be written to a delete manifest", ErrInvalidArgument)
			}
		case EntryContentPosDeletes, EntryContentEqDeletes:
			if w.content != ManifestContentDeletes {
				return fmt.Errorf("%w: delete file cannot be written to a data manifest", ErrInvalidArgument)
			}
		default:
			return fmt.Errorf("%w: invalid manifest entry content: %d", ErrInvalidArgument, entryContent)
		}
	}
	count := entry.DataFile().Count()

	var partition map[int]any
	entryToEncode := *entry
	if dataFile, ok := entry.DataFile().(*dataFile); ok {
		if err := dataFile.validateForWrite(); err != nil {
			return err
		}
		partition = dataFile.DataFilePartitionRef(internal.DataFileRef{})
		encodeDataFile := cloneDataFileAvroFields(dataFile)
		encodePartition, err := avroEncodePartitionData(
			partition, w.partFieldMaps)
		if err != nil {
			return err
		}
		encodeDataFile.PartitionData = encodePartition
		entryToEncode.Data = encodeDataFile
	} else {
		partition = entry.Data.Partition()
	}

	toEncode, err := w.impl.prepareEntry(&entryToEncode, w.snapshotID)
	if err != nil {
		return err
	}

	if err := w.writer.Encode(toEncode); err != nil {
		return err
	}

	switch status {
	case EntryStatusADDED:
		w.addedFiles++
		w.addedRows += count
	case EntryStatusEXISTING:
		w.existingFiles++
		w.existingRows += count
	case EntryStatusDELETED:
		w.deletedFiles++
		w.deletedRows += count
	}

	if w.partitionStatsErr == nil {
		w.partitionStatsErr = w.partitionStats.update(partition)
	}

	if status == EntryStatusADDED || status == EntryStatusEXISTING {
		if seq := entry.SequenceNum(); seq >= 0 && (w.minSeqNum < 0 || seq < w.minSeqNum) {
			w.minSeqNum = seq
		}
	}

	return nil
}

func (w *ManifestWriter) Add(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusADDED, &w.snapshotID, entry.(*manifestEntry).SeqNum, nil, entry.DataFile())

	return w.addEntry(&w.reusedEntry)
}

func (w *ManifestWriter) Delete(entry ManifestEntry) error {
	w.reusedEntry.wrap(EntryStatusDELETED, &w.snapshotID, entry.(*manifestEntry).SeqNum, entry.FileSequenceNum(), entry.DataFile())

	return w.addEntry(&w.reusedEntry)
}

func (w *ManifestWriter) Existing(entry ManifestEntry) error {
	snapshotID := entry.SnapshotID()
	w.reusedEntry.wrap(EntryStatusEXISTING, &snapshotID, entry.(*manifestEntry).SeqNum, entry.FileSequenceNum(), entry.DataFile())

	return w.addEntry(&w.reusedEntry)
}

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

func WriteManifest(
	filename string,
	out io.Writer,
	version int,
	spec PartitionSpec,
	schema *Schema,
	snapshotID int64,
	entries []ManifestEntry,
) (mf ManifestFile, err error) {
	cnt := &internal.CountingWriter{W: out}

	w, err := NewManifestWriter(version, cnt, spec, schema, snapshotID)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !w.closed {
			internal.CheckedClose(w, &err)
		}
	}()

	for _, entry := range entries {
		if err := w.addEntry(entry.(*manifestEntry)); err != nil {
			return nil, err
		}
	}

	// flush the writer to ensure cnt.Count is accurate
	if err := w.Close(); err != nil {
		return nil, err
	}

	return w.ToManifestFile(filename, cnt.Count)
}

// WriteManifestV3 writes a v3 data manifest and assigns first_row_id.
// The returned ManifestFile has FirstRowID() set to firstRowID.
// nextFirstRowID is firstRowID + AddedRowsCount + ExistingRowsCount
// for the caller to chain to the next manifest. Use this only for
// reconstruction — manifests committed through a v3 list writer get
// first_row_id from AddManifests; pre-setting collides with that allocation.
func WriteManifestV3(
	filename string,
	out io.Writer,
	firstRowID int64,
	spec PartitionSpec,
	schema *Schema,
	snapshotID int64,
	entries []ManifestEntry,
) (mf ManifestFile, nextFirstRowID int64, err error) {
	if firstRowID < 0 {
		return nil, 0, fmt.Errorf("%w: first row ID must be non-negative: %d", ErrInvalidArgument, firstRowID)
	}

	cnt := &internal.CountingWriter{W: out}

	w, err := NewManifestWriter(3, cnt, spec, schema, snapshotID)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if !w.closed {
			internal.CheckedClose(w, &err)
		}
	}()

	for _, entry := range entries {
		if err := w.addEntry(entry.(*manifestEntry)); err != nil {
			return nil, 0, err
		}
	}

	if err := w.Close(); err != nil {
		return nil, 0, err
	}

	mf, err = w.ToManifestFile(filename, cnt.Count)
	if err != nil {
		return nil, 0, err
	}

	raw := mf.(*manifestFile)
	v := firstRowID
	raw.FirstRowIDValue = &v
	nextFirstRowID, err = advanceRowID(firstRowID, raw.ExistingRowsCount, raw.AddedRowsCount)
	if err != nil {
		return nil, 0, fmt.Errorf("manifest %q: %w", filename, err)
	}

	return raw, nextFirstRowID, nil
}

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

// ManifestEntryStatus defines constants for the entry status of
// existing, added or deleted.
type ManifestEntryStatus int8

const (
	EntryStatusEXISTING ManifestEntryStatus = 0
	EntryStatusADDED    ManifestEntryStatus = 1
	EntryStatusDELETED  ManifestEntryStatus = 2
)

type ManifestEntryBuilder struct {
	m *manifestEntry
}

func NewManifestEntryBuilder(status ManifestEntryStatus, snapshotID *int64, data DataFile) *ManifestEntryBuilder {
	return &ManifestEntryBuilder{
		m: &manifestEntry{
			EntryStatus: status,
			Snapshot:    snapshotID,
			Data:        data,
		},
	}
}

func (b *ManifestEntryBuilder) SequenceNum(num int64) *ManifestEntryBuilder {
	b.m.SeqNum = &num

	return b
}

func (b *ManifestEntryBuilder) FileSequenceNum(num int64) *ManifestEntryBuilder {
	b.m.FileSeqNum = &num

	return b
}

func (b *ManifestEntryBuilder) Build() ManifestEntry {
	return b.m
}

type manifestEntry struct {
	EntryStatus ManifestEntryStatus `avro:"status"`
	Snapshot    *int64              `avro:"snapshot_id"`
	SeqNum      *int64              `avro:"sequence_number"`
	FileSeqNum  *int64              `avro:"file_sequence_number"`
	Data        DataFile            `avro:"data_file"`
}

func (m *manifestEntry) Status() ManifestEntryStatus { return m.EntryStatus }
func (m *manifestEntry) SnapshotID() int64 {
	if m.Snapshot == nil {
		return -1
	}

	return *m.Snapshot
}

func (m *manifestEntry) SequenceNum() int64 {
	if m.SeqNum == nil {
		return -1
	}

	return *m.SeqNum
}

func (m *manifestEntry) FileSequenceNum() *int64 {
	if m.FileSeqNum == nil {
		return nil
	}

	fileSeqNum := *m.FileSeqNum

	return &fileSeqNum
}

func (m *manifestEntry) DataFile() DataFile { return m.Data }

func (m *manifestEntry) inherit(manifest ManifestFile) {
	if m.Snapshot == nil {
		snap := manifest.SnapshotID()
		m.Snapshot = &snap
	}

	manifestSequenceNum := manifest.SequenceNum()
	if manifestSequenceNum != -1 {
		if m.SeqNum == nil && (manifestSequenceNum == initialSequenceNumber || m.EntryStatus == EntryStatusADDED) {
			m.SeqNum = &manifestSequenceNum
		}

		if m.FileSeqNum == nil && (manifestSequenceNum == initialSequenceNumber || m.EntryStatus == EntryStatusADDED) {
			m.FileSeqNum = &manifestSequenceNum
		}
	}

	m.Data.(*dataFile).specID = manifest.PartitionSpecID()
}

func (m *manifestEntry) wrap(status ManifestEntryStatus, newSnapID, newSeq, newFileSeq *int64, data DataFile) ManifestEntry {
	if newSeq != nil && *newSeq == -1 {
		newSeq = nil
	}

	m.EntryStatus = status
	m.Snapshot = newSnapID
	m.SeqNum = newSeq
	m.FileSeqNum = newFileSeq
	m.Data = data

	return m
}

type fallbackManifestEntry struct {
	manifestEntry
	Snapshot int64 `avro:"snapshot_id"`
}

func (f *fallbackManifestEntry) toEntry() *manifestEntry {
	f.manifestEntry.Snapshot = &f.Snapshot

	return &f.manifestEntry
}

func NewManifestEntry(status ManifestEntryStatus, snapshotID *int64, seqNum, fileSeqNum *int64, df DataFile) ManifestEntry {
	return &manifestEntry{
		EntryStatus: status,
		Snapshot:    snapshotID,
		SeqNum:      seqNum,
		FileSeqNum:  fileSeqNum,
		Data:        df,
	}
}

// ManifestEntry is an interface for both v1 and v2 manifest entries.
type ManifestEntry interface {
	// Status returns the type of the file tracked by this entry.
	// Whether entries with EntryStatusDELETED are returned depends on the
	// caller's discardDeleted option.
	Status() ManifestEntryStatus
	// SnapshotID is the id where the file was added, or deleted,
	// if null it is inherited from the manifest list.
	SnapshotID() int64
	// SequenceNum returns the data sequence number of the file.
	// If it was null and the status is EntryStatusADDED then it
	// is inherited from the manifest list.
	SequenceNum() int64
	// FileSequenceNum returns the file sequence number indicating
	// when the file was added. If it was null and the status is
	// EntryStatusADDED then it is inherited from the manifest list.
	FileSequenceNum() *int64
	// DataFile provides the information about the data file indicated
	// by this manifest entry.
	DataFile() DataFile

	inherit(manifest ManifestFile)

	wrap(status ManifestEntryStatus, snapshotID, seqNum, fileSeqNum *int64, datafile DataFile) ManifestEntry
}

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
	"cmp"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
)

// equalityDeleteFileSet names one immutable decoded delete file.
// File IDs are unique within one scan and permit compact, unambiguous cache keys.
type equalityDeleteFileSet struct {
	*equalityDeleteSet
	id       int
	groupKey string
}

func newEqualityDeleteFileSet(id int, deleteSet *equalityDeleteSet) *equalityDeleteFileSet {
	return &equalityDeleteFileSet{
		equalityDeleteSet: deleteSet,
		id:                id,
		groupKey:          fmt.Sprint(deleteSet.fieldIDs),
	}
}

// buildEqualityDeleteSetsPerTask shares read-only sets when scan tasks use the
// same equality delete files. Without sharing, copying each file's entire key
// map for each task uses O(tasks * delete keys) additional time and memory.
func buildEqualityDeleteSetsPerTask(
	tasks []FileScanTask,
	perFile map[string]*equalityDeleteFileSet,
) map[int][]*equalityDeleteSet {
	perTask := make(map[int][]*equalityDeleteSet)
	sharedSets := make(map[string]*equalityDeleteSet)

	for i, task := range tasks {
		if len(task.EqualityDeleteFiles) == 0 {
			continue
		}

		// A single delete file needs neither a merge nor a cache lookup.
		if len(task.EqualityDeleteFiles) == 1 {
			fileSet, ok := perFile[task.EqualityDeleteFiles[0].FilePath()]
			if ok && len(fileSet.keys) > 0 {
				perTask[i] = []*equalityDeleteSet{fileSet.equalityDeleteSet}
			}
			continue
		}

		groups := make(map[string][]*equalityDeleteFileSet)
		for _, file := range task.EqualityDeleteFiles {
			if fileSet, ok := perFile[file.FilePath()]; ok {
				groups[fileSet.groupKey] = append(groups[fileSet.groupKey], fileSet)
			}
		}

		sets := make([]*equalityDeleteSet, 0, len(groups))
		for _, files := range groups {
			// Canonicalize file order and ignore duplicate references.
			slices.SortFunc(files, func(a, b *equalityDeleteFileSet) int {
				return cmp.Compare(a.id, b.id)
			})
			files = slices.CompactFunc(files, func(a, b *equalityDeleteFileSet) bool {
				return a.id == b.id
			})

			var deleteSet *equalityDeleteSet
			if len(files) == 1 {
				deleteSet = files[0].equalityDeleteSet
			} else {
				// Fixed-width IDs prevent ambiguous keys, even when the same
				// files appear in a different order across scan tasks.
				keyBytes := make([]byte, 0, len(files)*8)
				for _, file := range files {
					keyBytes = binary.LittleEndian.AppendUint64(keyBytes, uint64(file.id))
				}
				key := string(keyBytes)
				deleteSet = sharedSets[key]

				if deleteSet == nil {
					// Copy the largest map once, then union the remaining maps.
					// Never mutate a per-file map: other tasks may share it.
					largest := 0
					for j := 1; j < len(files); j++ {
						if len(files[j].keys) > len(files[largest].keys) {
							largest = j
						}
					}

					keys := maps.Clone(files[largest].keys)
					for j, file := range files {
						if j == largest {
							continue
						}
						for key := range file.keys {
							keys[key] = struct{}{}
						}
					}

					deleteSet = &equalityDeleteSet{
						keys:     keys,
						fieldIDs: files[0].fieldIDs,
						colNames: files[0].colNames,
					}
					sharedSets[key] = deleteSet
				}
			}

			if len(deleteSet.keys) > 0 {
				sets = append(sets, deleteSet)
			}
		}
		if len(sets) > 0 {
			perTask[i] = sets
		}
	}

	return perTask
}

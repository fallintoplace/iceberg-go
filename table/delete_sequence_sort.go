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

import "slices"

// sortMonotonicBySequence puts an already monotonic bucket in ascending order
// without a stable O(n log n) sort. It returns false for mixed-order input, so
// the caller can use its original stable comparator without a regression in
// the common unordered case.
//
// Descending input needs special care: reversing the whole slice also reverses
// entries from the same snapshot. Reverse each equal-key run back to preserve
// their original order, matching slices.SortStableFunc exactly.
func sortMonotonicBySequence[T any](entries []T, sequenceNum func(T) int64) bool {
	if len(entries) < 2 {
		return true
	}

	ascending, descending := true, true
	hasTies := false
	previous := sequenceNum(entries[0])
	for i := 1; i < len(entries); i++ {
		current := sequenceNum(entries[i])
		switch {
		case previous < current:
			descending = false
		case previous > current:
			ascending = false
		default:
			hasTies = true
		}
		if !ascending && !descending {
			break
		}
		previous = current
	}

	if ascending {
		return true
	}
	if descending {
		slices.Reverse(entries)
		if hasTies {
			for start := 0; start < len(entries); {
				end := start + 1
				value := sequenceNum(entries[start])
				for end < len(entries) && sequenceNum(entries[end]) == value {
					end++
				}
				if end-start > 1 {
					slices.Reverse(entries[start:end])
				}
				start = end
			}
		}

		return true
	}

	return false
}

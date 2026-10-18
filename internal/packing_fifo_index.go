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

package internal

// fifoPackingIndex holds FIFO bins in a ring and indexes remaining capacity
// with a max segment tree. Searches return the oldest bin that fits, matching
// the first-fit ordering used by PackingIterator.
type fifoPackingIndex[T any] struct {
	bins          []Bin[T]
	maxRemaining  []int64
	head          int
	count         int
	targetWeight  int64
}

func newFIFOPackingIndex[T any](current []Bin[T], targetWeight int64) *fifoPackingIndex[T] {
	capacity := 1
	for capacity < len(current) {
		capacity *= 2
	}

	index := &fifoPackingIndex[T]{
		bins:         make([]Bin[T], capacity),
		maxRemaining: make([]int64, 2*capacity),
		count:        len(current),
		targetWeight: targetWeight,
	}
	copy(index.bins, current)
	index.rebuild()

	return index
}

func (s *fifoPackingIndex[T]) rebuild() {
	size := len(s.bins)
	for i := range size {
		s.maxRemaining[size+i] = -1
	}
	for i := range s.count {
		slot := (s.head + i) % size
		s.maxRemaining[size+slot] = s.targetWeight - s.bins[slot].binWeight
	}
	for i := size - 1; i > 0; i-- {
		s.maxRemaining[i] = max(s.maxRemaining[i*2], s.maxRemaining[i*2+1])
	}
}

func (s *fifoPackingIndex[T]) update(slot int) {
	node := len(s.bins) + slot
	s.maxRemaining[node] = s.targetWeight - s.bins[slot].binWeight
	for node > 1 {
		node /= 2
		s.maxRemaining[node] = max(s.maxRemaining[node*2], s.maxRemaining[node*2+1])
	}
}

func (s *fifoPackingIndex[T]) firstFitFrom(node, left, right, lowerBound int, weight int64) int {
	if right <= lowerBound || s.maxRemaining[node] < weight {
		return -1
	}
	if right-left == 1 {
		return left
	}

	mid := left + (right-left)/2
	if slot := s.firstFitFrom(node*2, left, mid, lowerBound, weight); slot >= 0 {
		return slot
	}

	return s.firstFitFrom(node*2+1, mid, right, lowerBound, weight)
}

func (s *fifoPackingIndex[T]) find(weight int64) int {
	if s.count == 0 || s.maxRemaining[1] < weight {
		return -1
	}
	if slot := s.firstFitFrom(1, 0, len(s.bins), s.head, weight); slot >= 0 {
		return slot
	}

	return s.firstFitFrom(1, 0, len(s.bins), 0, weight)
}

func (s *fifoPackingIndex[T]) grow() {
	oldSize := len(s.bins)
	grown := make([]Bin[T], oldSize*2)
	for i := range s.count {
		grown[i] = s.bins[(s.head+i)%oldSize]
	}
	s.bins = grown
	s.maxRemaining = make([]int64, oldSize*4)
	s.head = 0
	s.rebuild()
}

func (s *fifoPackingIndex[T]) add(item T, weight int64) {
	if s.count == len(s.bins) {
		s.grow()
	}

	slot := (s.head + s.count) % len(s.bins)
	s.bins[slot] = Bin[T]{targetWeight: s.targetWeight}
	s.bins[slot].Add(item, weight)
	s.count++
	s.update(slot)
}

func (s *fifoPackingIndex[T]) appendTo(slot int, item T, weight int64) {
	s.bins[slot].Add(item, weight)
	s.update(slot)
}

func (s *fifoPackingIndex[T]) removeOldest() Bin[T] {
	slot := s.head
	old := s.bins[slot]
	s.bins[slot] = Bin[T]{}
	s.count--
	s.head = (s.head + 1) % len(s.bins)

	// Empty slots cannot match a nonnegative item weight.
	node := len(s.bins) + slot
	s.maxRemaining[node] = -1
	for node > 1 {
		node /= 2
		s.maxRemaining[node] = max(s.maxRemaining[node*2], s.maxRemaining[node*2+1])
	}

	return old
}

// intoSlice returns bins in FIFO order when an unusual weight requires falling
// back to the original arithmetic, including its signed overflow behavior.
func (s *fifoPackingIndex[T]) intoSlice() []Bin[T] {
	result := make([]Bin[T], s.count)
	for i := range result {
		result[i] = s.bins[(s.head+i)%len(s.bins)]
	}

	return result
}

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

// CanonicalPartitionKey returns a collision-safe key for a partition spec and
// partition tuple. It uses the same type-normalized encoding as scan and commit
// validation. Unsupported partition value types return an error instead of
// silently combining unrelated partitions.
func CanonicalPartitionKey(specID int32, partition map[int]any) (string, error) {
	return canonicalPartitionKey(specID, partition)
}

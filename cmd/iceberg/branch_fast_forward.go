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

package main

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go/catalog"
)

// runBranchFastForward stages and validates an ancestry-preserving ref move.
// The explicit catalog commit supports the CLI's existing catalog injection
// and preserves the transaction's guards on both source and destination refs.
func runBranchFastForward(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchFastForwardCmd) {
	if err := validateRefName(cmd.BranchName); err != nil {
		output.Error(fmt.Errorf("invalid branch name: %w", err))
		osExit(1)
		return
	}
	if err := validateRefName(cmd.To); err != nil {
		output.Error(fmt.Errorf("invalid source ref name: %w", err))
		osExit(1)
		return
	}

	tbl := loadTable(ctx, output, cat, cmd.TableID)
	tx, err := tbl.NewTransactionOnBranch(cmd.BranchName)
	if err == nil {
		err = tx.FastForwardBranch(cmd.BranchName, cmd.To)
	}
	if err != nil {
		output.Error(err)
		osExit(1)
		return
	}

	commit, err := tx.TableCommit()
	if err != nil {
		output.Error(err)
		osExit(1)
		return
	}

	source, _ := findSnapshotRef(tbl.Metadata(), cmd.To)
	if len(commit.Updates) == 0 {
		output.Text(fmt.Sprintf("Branch %q is already at snapshot %d.", cmd.BranchName, source.SnapshotID))
		return
	}

	if err := confirmAction(
		fmt.Sprintf("Fast-forward branch %q on %s to ref %q at snapshot %d?",
			cmd.BranchName, tableIDString(tbl), cmd.To, source.SnapshotID),
		cmd.Yes,
	); err != nil {
		output.Error(err)
		osExit(1)
		return
	}

	if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), commit.Requirements, commit.Updates); err != nil {
		output.Error(fmt.Errorf("failed to fast-forward branch %q: %w", cmd.BranchName, err))
		osExit(1)
		return
	}

	output.Text(fmt.Sprintf("Fast-forwarded branch %q on %s to snapshot %d.",
		cmd.BranchName, tableIDString(tbl), source.SnapshotID))
}

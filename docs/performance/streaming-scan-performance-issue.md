# perf(table): stream scan tasks into bounded-memory Arrow execution

## Summary

Stream scan tasks into Arrow execution with **bounded planning memory**, instead of materializing a complete `[]FileScanTask` before the first row can be read. Keep the existing eager `Scan.PlanFiles` API unchanged. Start with an opt-in streaming scan path so callers can explicitly accept planning errors that may arrive during iteration.

**Target repository:** `apache/iceberg-go`  
**Investigated revision:** [`7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9`](https://github.com/apache/iceberg-go/commit/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9), main, October 21, 2026.

## Problem and code evidence

- [`Scan.ToArrowRecords`](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/scanner.go#L2118-L2126) calls `scan.planFiles(ctx, true)` and only then invokes `ReadTasks`. The first data-file read waits for *all* file planning.
- [`planDataManifestTasksWithOptions`](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/scanner.go#L1387-L1565) streams Avro manifest entries into per-manifest tasks, but still creates a potentially huge shared task buffer or per-manifest arrays, flattens the arrays, and hands the full task set to finalization.
- [`arrowScan.GetRecords`](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/arrow_scanner.go#L3152-L3224) builds projection and deletion-loader state from the complete slice. [The scan worker task queue](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/arrow_scanner.go#L3044-L3149) is already bounded, but only after the large task plan is retained.
- [REST scan planning](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/catalog/rest/scan_planning.go#L197-L352) expands and stores all plan-task envelopes and resolved tasks before returning them.

Even a caller that wants just one batch can pay to plan every data file and split task. The retained task set grows with file count, split count, and referenced deletes; time to first row includes all manifest reads and finalization.

This does **not** duplicate merged [#1974](https://github.com/apache/iceberg-go/pull/1974), which removed the temporary global manifest-entry collection but still returns a complete task slice, merged [#2039](https://github.com/apache/iceberg-go/pull/2039), which bounded decoded-batch reordering after planning, or merged [#2149](https://github.com/apache/iceberg-go/pull/2149), which parallelized finalization but still requires an eager plan.

## Proposed implementation

1. **Opt-in streaming execution.** Keep `Scan.PlanFiles(context.Context) ([]FileScanTask, error)` and its existing behavior. Add an internal ordered `iter.Seq2[FileScanTask, error]` or similar task producer, with an opt-in Arrow scan entry point or option. Do not silently change the default `ToArrowRecords` error contract: a streamed manifest error may arrive during iteration instead of synchronously during setup.
2. **Local planning.** Resolve snapshot/schema and build delete indexes first as required by Iceberg's delete matching rules. Stream data manifest entries through partition evaluation, residual calculation, matching positional/equality/DV delete references, and Parquet splitting into a bounded task window. Avoid constructing a global `[]FileScanTask` in the streaming path.
3. **Ordering and backpressure.** Preserve manifest and task order even when later manifest reads complete first. Use credits for in-flight manifests or bounded staging before producing tasks so later producers cannot accumulate unlimited memory behind a slow earlier manifest. Do not hold a global lock across IO or consumer waits. Keep the existing decoded-batch reordering bound.
4. **Reader integration.** Initialize invariant projected schema/filter state once and register per-task residual and deletion references as task windows arrive. Preserve shared delete-file reuse, sequence numbers and partition scoping; do not load delete files for tasks that will never be consumed. Existing `ReadTasks` over a caller-owned slice remains unchanged.
5. **Remote planning.** Stream inline, paginated and recursive plan-task envelopes with bounded concurrent fetches and buffered results. Preserve envelope-local delete references, plan-scoped FileIO leases, storage credentials and task ordering. Cancel or release remote plans after successful completion, error, cancellation or early break.
6. **Metrics and early termination.** Keep scan report counts and durations correct; explicitly distinguish partial scans from fully planned scans if metrics change. Stop fetching unneeded manifests when a consumer breaks or a row limit is satisfied. Preserve all already-emitted rows and ensure all goroutines and resources are cleaned up.

**Implementation stages:** (A) local streaming with no delete files; (B) delete-aware local streaming; (C) REST continuation planning. Keep eager `PlanFiles` as the correctness control throughout. Determine API/error semantics with maintainers before changing any default behavior.

## Full benchmark specification

### Measurement status

This is a **reproducible benchmark plan**, not an invented before/after result. Capture baseline results on the pinned main revision **before** implementing the streaming path. Copy the exact same benchmark fixture into both base and implementation worktrees so the binaries run identical workloads.

For background only, [#1974](https://github.com/apache/iceberg-go/pull/1974) measured 8 manifests x 10,000 entries at **28.41 ms/op and 86.94 MB allocated/op** before its manifest-entry streaming change versus **23.73 ms/op and 80.68 MB allocated/op** afterward. Those are historical *cumulative allocated bytes*, not peak live heap, not the current main baseline, and not a predicted gain from this new proposal.

### Benchmark fixture

Extend the actual Avro manifest and in-memory FileIO fixtures in [`scanner_row_limit_bench_test.go`](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/scanner_row_limit_bench_test.go), [`scan_task_finalization_planfiles_bench_test.go`](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/scan_task_finalization_planfiles_bench_test.go), and [`arrow_scanner_bench_test.go`](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/arrow_scanner_bench_test.go).

Generate the snapshot, manifest lists, manifests, file paths and Parquet input **outside the timed loop**. Use real Iceberg manifest metadata and deterministic unique logical data-file paths, with reproducible ordering and a deterministic FileIO. Keep input fixture buffers live and exclude their own allocation from the scan's reported heap delta.

#### Group A: large planning and peak memory

- Data-file count: **1,000 / 10,000 / 100,000 / 1,000,000**.
- Manifest size: **1,000 / 10,000 entries per manifest**, varying manifest count independently from file count.
- Split tasks: **1 or 8 per data file**, with valid Parquet split offsets. Measure the additional task explosion separately from file count.
- Filter: `AlwaysTrue` and an identity-partition predicate retaining approximately 1% of files.
- Delete references: none, positional, equality, or deletion-vector. Keep delete-index size fixed when measuring how memory scales with *data-file* count.
- Concurrency: **1 / 4 / 16 workers**. Streaming window size: **1x / 2x / 4x worker count**.
- Controls: eager `Scan.PlanFiles`, eager `ToArrowRecords`, and the new opt-in streamed execution. Compare the same work, not a metadata-only benchmark against a full-read benchmark.
- The 1-million-file case may intentionally be metadata-only to avoid physically writing and reading a million Parquet files. Label this separately and do not mix its throughput with end-to-end results.

#### Group B: end-to-end Arrow records

- Data-file count: **1,000 / 10,000 / 100,000**; use small valid Parquet files or unique logical file paths backed by the same immutable valid Parquet bytes in a test FileIO.
- **1 / 8 row groups** per file, **8 / 32** projected columns, with and without Parquet split planning.
- Unpartitioned and identity-partitioned tables, plus partition-spec evolution.
- Filters retaining about **100% or 1%** of files.
- Delete density: **0%, 1%, 20%** referenced data files, testing positional, equality and v3 deletion-vector paths independently.
- Consumer behavior: **first Arrow batch**, **first 1,000 rows**, **full drain**, and **early break immediately after one batch**.
- Slow-head case: artificially block the first manifest or data-file task while later ones complete, then release it. Measure bounded staging and verify stable ordering and lack of deadlock.
- FileIO latency: zero latency, **1 ms** and **10 ms** per manifest fetch, reported in separate warm/cold categories.
- REST mock server: inline tasks, recursive plan-task handles, paginated envelopes, scoped credentials, cancellation, and error injection. Count concurrent fetches, buffered envelopes, plan cleanup calls and bytes retrieved.

### Metrics to collect on every workload

- `ns/op`, `B/op`, `allocs/op`, records/sec, tasks/sec, and number of files/splits consumed.
- **Time to first Arrow batch** from the scan call, including planning, with raw samples and p50/p95 across repetitions. Also report full-drain wall-clock time.
- **Peak incremental live heap**, peak `HeapInuse`, peak process RSS and post-GC retained heap. Record the sampling interval and `GOGC` setting; cumulative `B/op` is **not** peak heap. Include Arrow allocator bytes separately when supported.
- Maximum queued tasks, concurrent manifest buffers, goroutine count, FileIO handles, REST fetches and in-flight continuation envelopes.
- File-open and bytes-read counts, result row counts and hashes, task-order equality, schema/projection/residual parity, and deletion semantics.
- Time from early break or cancellation to resource cleanup; all Arrow batches released, no stranded writers/readers and no leaked remote plan-scoped IO.

Do not pool results across different CPU/OS/Go versions or different caches. Repeat with both low-latency memory FileIO and latency-injected FileIO. Keep a fixed manifest and Parquet fixture across base and candidate binaries.

### Benchmark commands

Add identical `BenchmarkStreamingScanExecution` and `BenchmarkStreamingScanPeakMemory` benchmark fixtures in both base and candidate worktrees. Suggested commands:

```bash
# In both worktrees, with an unchanged copy of the benchmark fixture:
go test -c -o /tmp/iceberg-scan-BUILD.test ./table

# Replace BUILD with base or candidate; save outputs separately.
GOMAXPROCS=8 /tmp/iceberg-scan-BUILD.test \
  -test.run='^$' \
  -test.bench='^BenchmarkStreamingScanExecution$' \
  -test.benchmem -test.benchtime=2s -test.count=5 -test.cpu=8 \
  > BUILD.txt

benchstat base.txt candidate.txt

# Peak memory: one complete large scan per sample.
GOMAXPROCS=8 /tmp/iceberg-scan-BUILD.test \
  -test.run='^$' \
  -test.bench='^BenchmarkStreamingScanPeakMemory$' \
  -test.benchtime=1x -test.count=5 -test.cpu=8
```

Interleave runs in base/candidate/candidate/base order when possible to minimize temperature and frequency drift. Use `benchstat` for time/allocations and report all five memory samples plus their median rather than reporting only one peak number. For the isolated memory benchmark, trigger GC just before entering the measured scan, then sample live heap/RSS until the iterator closes. Do **not** run a forced GC inside timed throughput iterations.

Also rerun existing `BenchmarkPlanFilesTaskFinalization`, `BenchmarkPlanFilesWithRowLimit`, and `BenchmarkArrowScanManyFilesAndBatches` to catch eager path and small-scan regressions. Run the relevant tests with `-race` on both `./table` and `./catalog/rest`.

### Acceptance targets, not observations

1. **At least 70% lower peak incremental scan heap** at 100K files with no/few deletes versus eager `ToArrowRecords`.
2. Streaming task-buffer retention must scale with **configured window + concurrently decoded manifest entries**, not total file/split count. Manifest-list and legitimate delete-index memory may still scale separately; measure them explicitly.
3. **At least 2x faster first-batch time** with 100K files when early manifests are ready substantially before the final manifest, especially with injected remote latency.
4. Full-drain throughput no worse than **5%** versus eager for balanced 4-worker and 16-worker scans. If tighter memory requires a throughput tradeoff, document the window-size Pareto curve.
5. On early break, error or cancellation, no leaked Arrow references, goroutines, FileIO handles, remote plan-scoped credentials or server-side planning sessions.
6. `Scan.PlanFiles` retains the same public signature, metadata completeness, ordering and error behavior.

### Correctness / regression tests

- Ordered output despite out-of-order manifest and data-file completion; stress a slow first manifest.
- Mixed partition specs, evolving schemas, partition residuals, nulls, stats pruning and split offsets.
- Position/equality deletes, partition scope, strictly-greater equality-delete sequence matching, shared delete-file references, DVs and v3 row lineage.
- `WithRowFilter`, projection, zero/one row limits, empty/pruned manifests, no snapshot and malformed metadata.
- Cancellation while the producer is blocked on a full task queue, during manifest IO, during Parquet read, and while a REST fetch is pending. No deadlock or goroutine leak.
- Early iterator break before the final manifest is read; ensure no unnecessary future manifest reads.
- REST plan cancellation, expired handles, nested continuation envelopes, and plan-scoped FileIO lifetime.
- Compare row counts/hashes and full-task identity/order for completely consumed scans to the eager path.
- `go test -race ./table ./catalog/rest` and existing scan/REST conformance tests.

## Related work

- [#1974 - Stream data manifest entries into tasks](https://github.com/apache/iceberg-go/pull/1974)
- [#2039 - Bound scan reorder heap](https://github.com/apache/iceberg-go/pull/2039)
- [#2149 - Parallelize scan task finalization](https://github.com/apache/iceberg-go/pull/2149)
- [#2147 - Cache manifest content across scans](https://github.com/apache/iceberg-go/pull/2147)
- [#1178 - REST catalog scan-planning epic](https://github.com/apache/iceberg-go/issues/1178)

**Maintainer design question:** should this be a new opt-in streaming API or an internal implementation selected by an existing option? The observable timing of manifest and REST planning errors is the key compatibility tradeoff.

# perf(table): page-index-aware Parquet scans with page skipping and late projection

## Summary

Add conservative **Parquet page-index pruning** and **page-aware late projection** to Iceberg Go scans. The goal is to avoid fetching and decoding irrelevant pages *inside row groups that survived the existing row-group pruning pass*, while keeping row order, deletes, row lineage, split-task ownership, schema evolution and the existing full-scan fast path unchanged.

This is a proposal for a large, measurable performance improvement. **No before/after speedup has been measured yet.** The benchmark design, controls and reporting requirements below are intended to establish the baseline and validate the implementation. All numerical improvement thresholds in this issue are proposed acceptance targets, **not observed results**.

## Problem and code path

`wrapPqArrowReader.GetRecords` currently applies split-range selection and row-group checks based on statistics, dictionary contents and Bloom filters. It then passes surviving row-group IDs to `GetRecordReader(ctx, cols, rgList)`:

- [Iceberg Go reader implementation, lines 2102-2255](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/internal/parquet_files.go#L2102-L2255)
- [Existing large-file and split-task benchmarks](https://github.com/apache/iceberg-go/blob/7eea50c9b7dcbe5bc2644b38a4e9f216b2f66ab9/table/arrow_scanner_bench_test.go)

There is no page-index selection in that code path. A surviving row group with 128 MiB of data can still require substantial reads and decompression even if a selective query matches only a small fraction of its pages. Wide projections compound this cost. Extra object-store reads and latency can dominate on S3/GCS/Azure.

The [Parquet page-index specification](https://parquet.apache.org/docs/file-format/pageindex/) defines:

- `ColumnIndex`: per-page min/max values and null information.
- `OffsetIndex`: page locations and first-row positions.

[Arrow Go already exposes page-index readers](https://github.com/apache/arrow-go/blob/main/parquet/metadata/page_index.go) and lower-level column-reader seek APIs. However, the current `pqarrow.FileReader.GetRecordReader` accepts **column and row-group lists**, not arbitrary candidate row intervals. Therefore, **only checking page-index metadata is not enough**. To deliver a useful performance gain, the implementation must genuinely avoid I/O and decoding for rejected page ranges, potentially with a small upstream Arrow Go reader API or an isolated adapter.

## Proposed implementation

### 1. Build a conservative page candidate plan

1. Run existing file, split-range, row-group statistics, dictionary and Bloom pruning first. Check page indexes only for the remaining row groups and relevant physical filter columns.
2. Use the existing Iceberg schema/field-ID binding and expression translation. Interpret Parquet page min/max with type-correct comparisons and Iceberg's conservative bounds rules, including nulls, signed values, decimal scale, timestamp representation, byte ordering, truncated string/binary bounds and NaN/negative zero behavior.
3. Start with `EqualTo`, `IN`, bounded ranges and safe null predicates where conservative proof is possible. Intersect candidate row intervals for `AND` conjuncts. Do not prune based on unsupported operations; add `OR` only with a proved conservative union and avoid `NOT` unless complement semantics are safe.
4. Convert matching pages to **row-group-local row intervals** using `OffsetIndex`. Sort/coalesce adjacent intervals before passing them to the reader. Index-derived intervals may include false positives, never false negatives.
5. Treat missing indexes, incomplete index pairs, suspicious page ordering, unsupported column types, nested/repeated layouts without correct row accounting and invalid/incompatible metadata conservatively. Preserve existing error behavior for genuine corruption; do not silently swallow read errors unrelated to optional index pruning.
6. Cache the **per-scan** page plan if multiple projections/adjacent split tasks reuse it, but cap its size and avoid global unbounded caches.

### 2. Physically skip pages and defer payload projection

1. Read/decompress only candidate row intervals. Avoid materializing non-filter projected columns outside those intervals. Preserve aligned Arrow arrays and record batches when columns use different page boundaries.
2. **Always run the exact record-level predicate afterward.** Page bounds cannot replace actual filtering.
3. Preserve the original **absolute row position within the data file**, across skipped pages, row groups and split tasks. Position deletes and deletion vectors refer to original positions, not to the compacted candidate-record index.
4. Maintain equality-delete application, row IDs/lineage, schema reconciliation, nullable/missing/default fields and user-visible row order exactly as the current scanner does.
5. If Arrow Go's public `pqarrow` APIs cannot skip pages at the requested row granularity, implement the necessary Arrow Go extension first or use a well-encapsulated reader adapter. A solution that first reads full projected row groups and only filters afterward does **not** meet this ticket's goal.
6. Coalesce nearby requested ranges and bound in-flight buffers proportional to scan concurrency. Ensure early iterator exit and cancellation release all Arrow references and close readers.

### 3. Use a cost-aware fallback

1. Estimate candidate-page coverage and additional index I/O. Bypass page-aware reads for high-selectivity scans, many disjoint tiny ranges, unavailable indexes, narrow projections, small row groups or other cases where seeking is likely slower than sequential reads.
2. Keep the no-filter, no-index, random-key and full-scan paths free of repeated page-index reads or avoidable allocations.
3. Make the feature opt-in/internal until end-to-end parity and regression data support enabling it broadly. Add a public table property only if it solves a concrete user requirement.
4. Add bounded-cardinality diagnostic counters: row groups checked/skipped, index bytes fetched, candidate pages/pages skipped, data bytes fetched, read operations, decoded rows, emitted rows and fallback reason.

## Complete benchmark plan

### A. Fixture generation

Generate reproducible Arrow/Parquet fixtures **outside the timed loop**, using fixed seed `42` and a deterministic generator that streams batches instead of materializing the full file. Save fixture hashes and full writer settings. For indexed fixtures, explicitly use `parquet.WithPageIndexEnabled(true)` in the Arrow Go writer; write otherwise equivalent no-index fixtures separately. Report **actual** file sizes, page counts and row-group sizes from the resulting footers, not merely requested targets.

Benchmark axes:

- **Compressed data volume:** 256 MiB for full local profiling and 2 GiB for opt-in stress/object-store runs. A 32-64 MiB fixture should be enough for CI correctness tests.
- **Row-group target:** 16 MiB and 128 MiB uncompressed, plus a deliberately large single-row-group case.
- **Page target:** 64 KiB and 1 MiB with a 20,000-row page-limit variant.
- **Schema width:** 8, 32 and 128 columns. Primary performance measurements use primitive `INT32`, `INT64`, string/binary, decimal, timestamp and nullable payloads; nested/list/map behavior gets a separate correctness matrix.
- **Value distribution:** ascending sorted key, descending sorted key, clustered key with overlapping page bounds, uniform random key and low-cardinality dictionary key.
- **Projection:** predicate column only; predicate + 2 payload columns; predicate + 8 columns; and all columns.
- **Index state:** complete ColumnIndex/OffsetIndex; no page indexes; partial index pairs; missing/null statistics; malformed or mismatched metadata for fallback/error tests.
- **Predicate:** no filter; an absent value *inside the row-group min/max range*; equality; `IN` with 4 or 64 literals; inclusive/exclusive ranges; safe `IS NULL`; `AND`; and unsupported expressions that force a conservative fallback.
- **Actual selectivity:** 0%, 0.01%, 0.1%, 1%, 10%, 50% and 100%. Record the exact matched row count.

**Critical isolation:** at least one fixture must have a surviving row group whose min/max overlaps the predicate even though most pages cannot match. Otherwise current row-group pruning wins before page pruning runs, and the benchmark does not measure the proposed feature.

For random keys, page min/max intervals should overlap heavily. This is a mandatory negative-control workload: checking indexes and issuing many ranges may be worse than sequential reading.

### B. Page-planning microbenchmark

Add `BenchmarkParquetPageIndexCandidatePruning` under `table/internal` to measure candidate interval planning separately from Arrow decoding.

Include 1, 16 and 128 row groups; 1, 16 and 256 pages per group; 1, 4 and 16 filter columns; and sorted, clustered, random, missing-index and partial-index paths.

Report `ns/op`, `B/op`, `allocs/op`, `pages_checked/op`, `candidate_pages/op`, `index_bytes/op` and plan bytes. Separate cold initial index fetch/parse from warm reuse. No fixture generation or setup within the timed loop. Use a sink to prevent dead-code elimination and assert the candidate interval count for each fixture.

### C. Public `Scan.ReadTasks` end-to-end benchmark

Add `BenchmarkParquetPageIndexReadTasks` in `table` to exercise the **public scan reader and full iterator consumption**, including batch release, projection, exact predicate filtering, and optional delete application.

- Compare *optimization disabled* vs *enabled* with **identical** files, scan tasks, filters, projection, concurrency and machine configuration.
- Use `LocalFS` or a controlled counting `ReaderAt` instead of a benchmark `MemFS` that copies whole files on open. The existing `table/arrow_scanner_bench_test.go` documents this concern.
- Cover 1 and 32 files, concurrency 1 and 8, whole-file tasks and split row-group tasks, and full projection vs 2-column projection.
- Include explicit runs for no-filter/no-index, high-selectivity, randomly distributed values, sorted 0.01%/0.1%, and clustered 1%/10%.
- Consume every batch, validate row counts/schema and checksum of exact values, then `Release()` it. Assert parity on every measured operation so an accidentally empty result cannot appear fast.
- Instrument `ReaderAt`/HTTP transport to report **actual physical requested bytes** and request counts. Lower CPU time without fewer bytes is not evidence of page-level I/O skipping.

Required metrics: `ns/op`, `B/op`, `allocs/op`, decoded rows/op, returned rows/op, `rows/s`, fetched bytes/op, read requests/op, pages skipped/op and peak Arrow allocator bytes. Record peak RSS separately for opt-in large runs.

### D. Object-store benchmark with realistic latency

Use the existing MinIO/integration setup where practical and, optionally, one real in-region S3, GCS or Azure Blob storage run. Keep object paths, credentials, region, retry policy and cache settings fixed between variants.

Workloads:

- 2 GiB of compressed wide Parquet data, ascending-sorted predicate key, page indexes present; 0.1% and 10% exact selectivity.
- Same data with page-index pruning disabled; plus an otherwise equivalent unindexed control.
- 0 ms, 10 ms and 50 ms per-request latency. Distinguish **injected latency** from real network latency.
- Scan workers 1, 4 and 8; cold and warm reader/object-cache conditions.
- A random-key/50%-selectivity control that should use the ordinary read path if the cost model predicts better performance.

Record wall time to first batch, complete scan duration, rows/s, response bytes, GET/range request counts, retries/throttling, CPU time, peak RSS, peak Arrow allocator bytes and index bytes fetched. Measure whether coalescing reduces excessive tiny ranges. Report both data bytes and total bytes including indexes.

Repeat paired runs under the same network conditions, and disclose if remote variability makes throughput differences statistically inconclusive.

### E. Correctness, compatibility and adversarial tests

Differentially compare enabled/disabled page-aware reads for **the exact same tasks and input files**. Required scenarios:

- Position deletes, multiple positional delete files, deletion vectors, equality deletes and combinations of delete types.
- Original row positions and row IDs after skipping middle pages and entire row groups, including split tasks starting after row zero.
- Nullable values, all-null pages, null-containing dictionaries, nested/repeated/list/map columns and page boundaries that split repeated values. Use the conservative legacy path where row semantics cannot be preserved.
- Iceberg schema evolution, field-ID mapping, renamed/reordered columns, missing columns/defaults and mixed Parquet writer implementations.
- Unsigned-looking encoded values with signed Iceberg semantics, decimals, timestamps, binary/string bounds, truncated min/max, NaN and signed zero.
- `AND`/`OR`/`NOT` expressions and predicates unsupported by page-level pruning; verify safe fallback without incorrect negative results.
- Parquet DataPage V1/V2, different compression codecs, dictionary/plain pages, index absent in only some row groups, malformed index offsets and truncated footer data.
- Iceberg v1/v2/v3 where supported by the scanner, cancellation, early iterator stop, concurrent scans, race detection, reader closing and Arrow allocator leak checks.

**Zero false negatives is mandatory.** Every uncertain page is retained. Invalid optional metadata must never cause a silent loss of matching rows.

### F. Reproducible commands and result reporting

First add/cherry-pick the **identical benchmark harness** on both base and candidate commits. Never compare a newly optimized benchmark to an unrelated historical number. Pin exact Go version, base/head SHAs, OS/architecture, CPU model, `GOMAXPROCS`, heap settings, dataset hashes, Parquet page/row-group settings, storage backend and cache mode.

Proposed benchmark names below must be implemented as part of this work:

```bash
# Base commit with the shared benchmark harness, 10 repetitions
GOMAXPROCS=4 go test ./table ./table/internal \
  -run '^$' -bench '^BenchmarkParquetPageIndex' \
  -benchtime=2s -count=10 -benchmem -timeout=30m \
  | tee /tmp/page-index-before.txt

# Candidate commit with the exact same benchmark harness and fixtures
GOMAXPROCS=4 go test ./table ./table/internal \
  -run '^$' -bench '^BenchmarkParquetPageIndex' \
  -benchtime=2s -count=10 -benchmem -timeout=30m \
  | tee /tmp/page-index-after.txt

benchstat /tmp/page-index-before.txt /tmp/page-index-after.txt

# Focused correctness, leak and race checks, in addition to normal tests
# Adapt focused test patterns to actual test names added by the implementation.
go test ./table ./table/internal -run 'PageIndex|Scan|Delete' -count=1
go test -race ./table ./table/internal -run 'PageIndex' -count=1
```

Run before/after in an alternating order where practical to reduce thermal, filesystem cache and machine load bias; report median/mean and statistical variability instead of a cherry-picked best run. Profile representative regressions and wins using `-cpuprofile`, `-memprofile` and `go tool pprof`. Keep 2 GiB and real object-store runs out of routine CI.

**Every key before/after result in the implementation PR must show:**

- Fixture identifier and hash, size, row groups, pages, schema width, selectivity, projection, task mode, workers, storage, cache state and RTT.
- Baseline and optimized `ns/op` and percentage/speedup **with variability**, `B/op`, `allocs/op`, bytes requested, I/O request counts, index bytes, decoded rows and returned rows.
- Where relevant: first-batch latency, full duration, rows/s, peak RSS and peak Arrow memory.
- Data correctness/parity checksum, index coverage and fallback decision, plus regressions and explanations.
- The exact commands, Go version, CPU, base/head commits and dataset reproducibility notes.

**Benchmark results:** not measured yet. Do not insert estimated data into the eventual PR as if measured.

### G. Proposed acceptance targets (not measured outcomes)

- For sorted or clustered, wide-file scans with `<= 0.1%` selectivity and usable page indexes, aim for **at least 60% fewer physically fetched data bytes** and **2x end-to-end throughput** in a representative object-store scenario. Treat these as testable goals and adjust based on baseline findings.
- For no-filter, no-index, full-scan and high-selectivity/random cases, **no repeatable >5% wall-time regression**, unless clearly explained and explicitly accepted; avoid a page-index tax on fallback paths.
- Keep peak in-flight memory bounded with scan concurrency rather than total pages or file count.
- Require complete correctness parity, particularly delete semantics, absolute row positions, schema/row order and no Arrow reference leaks.
- If a synthetic microbenchmark improves but **physical data bytes requested do not drop** on selective indexed reads, the core goal has not been met.

## Related work and boundaries

- [#2006](https://github.com/apache/iceberg-go/pull/2006): merged **dictionary row-group pruning**. This proposal handles pages **inside accepted row groups**.
- [#1939](https://github.com/apache/iceberg-go/pull/1939): **row-group task splitting**, not page skipping.
- [#601](https://github.com/apache/iceberg-go/pull/601): closed, unmerged **page-index writer property** proposal; read-side optimization must also work on indexed Parquet written by others.
- [Arrow Go page-index metadata interfaces](https://github.com/apache/arrow-go/blob/main/parquet/metadata/page_index.go) and the [Parquet page-index spec](https://parquet.apache.org/docs/file-format/pageindex/).
- [Existing Iceberg Go scan benchmarks](https://github.com/apache/iceberg-go/blob/main/table/arrow_scanner_bench_test.go) for fixture design and benchmarking conventions.

**Non-goals:** new manifest indexes, changing snapshot planning, caching whole Parquet files, relying on approximate indexes instead of exact row/deletion filtering, or introducing extra index work into ordinary full scans.

## Definition of done

- [ ] Candidate page intervals are conservatively derived after row-group filtering.
- [ ] Selected reads physically bypass irrelevant pages of both predicate and payload columns.
- [ ] Split ownership, original row positions, deletes, lineage and schema semantics remain identical.
- [ ] Cost-aware fallback protects unindexed, high-selectivity and full-read performance.
- [ ] Microbenchmarks, public `ReadTasks` benchmarks and object-store measurements are reproducible with full before/after data.
- [ ] No unbounded in-flight memory, race, cancellation or Arrow lifetime regressions.

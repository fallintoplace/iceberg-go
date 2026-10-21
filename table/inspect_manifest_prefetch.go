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
	"bytes"
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"sync"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

const (
	// Bound both the number of in-flight reads and their retained byte buffers.
	inspectManifestPrefetchConcurrency = 8
	inspectManifestPrefetchMaxBytes    = 4 << 20
)

// inspectManifestReadSource pairs a manifest with the filesystem containing
// its bytes. The manifest is always decoded in order by the caller.
type inspectManifestReadSource struct {
	manifest iceberg.ManifestFile
	fileIO   iceio.IO
}

type inspectManifestReadResult struct {
	source inspectManifestReadSource
	err    error
}

// inspectManifestPrefetchSource overlaps remote manifest reads with decoding
// previous manifests, without buffering decoded entries or Arrow batches.
// Only manifests up to the byte limit are prefetched. A large manifest stays
// on the original streaming path. At most concurrency manifests are in flight.
func inspectManifestPrefetchSource(
	ctx context.Context,
	fileIO iceio.IO,
	source iter.Seq2[iceberg.ManifestFile, error],
	includeManifest func(iceberg.ManifestFile) bool,
	concurrency int,
) iter.Seq2[inspectManifestReadSource, error] {
	if concurrency <= 1 {
		return func(yield func(inspectManifestReadSource, error) bool) {
			for manifest, err := range source {
				if err != nil {
					yield(inspectManifestReadSource{}, err)

					return
				}
				if includeManifest != nil && !includeManifest(manifest) {
					continue
				}
				if !yield(inspectManifestReadSource{manifest: manifest, fileIO: fileIO}, nil) {
					return
				}
			}
		}
	}

	return func(yield func(inspectManifestReadSource, error) bool) {
		readCtx, cancel := context.WithCancel(ctx)
		var workers sync.WaitGroup
		defer workers.Wait()
		defer cancel()

		next, stop := iter.Pull2(source)
		defer stop()

		pull := func() (iceberg.ManifestFile, error, bool) {
			for {
				manifest, err, ok := next()
				if !ok || err != nil || includeManifest == nil || includeManifest(manifest) {
					return manifest, err, ok
				}
			}
		}

		first, firstErr, ok := pull()
		if !ok {
			return
		}
		second, secondErr, ok := pull()
		if !ok {
			// Preserve the existing zero-copy streaming behavior for a single
			// manifest. The prefetch pool pays off only with multiple reads.
			yield(inspectManifestReadSource{manifest: first, fileIO: fileIO}, firstErr)

			return
		}

		pending := make([]<-chan inspectManifestReadResult, 0, concurrency)
		start := func(manifest iceberg.ManifestFile, sourceErr error) {
			result := make(chan inspectManifestReadResult, 1)
			pending = append(pending, result)
			if sourceErr != nil {
				result <- inspectManifestReadResult{err: sourceErr}

				return
			}
			if manifest.Length() <= 0 || manifest.Length() > inspectManifestPrefetchMaxBytes {
				result <- inspectManifestReadResult{
					source: inspectManifestReadSource{manifest: manifest, fileIO: fileIO},
				}

				return
			}

			workers.Add(1)
			go func() {
				defer workers.Done()

				if err := readCtx.Err(); err != nil {
					result <- inspectManifestReadResult{err: err}

					return
				}
				bufferedIO, err := readInspectManifestBytes(fileIO, manifest)
				result <- inspectManifestReadResult{
					source: inspectManifestReadSource{manifest: manifest, fileIO: bufferedIO},
					err:    err,
				}
			}()
		}

		start(first, firstErr)
		start(second, secondErr)
		exhausted := secondErr != nil
		for len(pending) < concurrency && !exhausted {
			manifest, err, ok := pull()
			if !ok {
				exhausted = true

				break
			}
			start(manifest, err)
			if err != nil {
				exhausted = true
			}
		}

		for len(pending) > 0 {
			result := <-pending[0]
			pending[0] = nil
			pending = pending[1:]

			if !exhausted {
				manifest, err, ok := pull()
				if ok {
					start(manifest, err)
					if err != nil {
						exhausted = true
					}
				} else {
					exhausted = true
				}
			}

			if result.err != nil {
				yield(inspectManifestReadSource{}, result.err)

				return
			}
			if !yield(result.source, nil) {
				return
			}
		}
	}
}

type inspectManifestBufferedIO struct {
	iceio.IO
	path string
	data []byte
	info fs.FileInfo
}

func (b inspectManifestBufferedIO) Open(path string) (iceio.File, error) {
	if path != b.path {
		return b.IO.Open(path)
	}

	return &inspectManifestBufferedFile{
		Reader: bytes.NewReader(b.data),
		info:   b.info,
	}, nil
}

type inspectManifestBufferedFile struct {
	*bytes.Reader
	info fs.FileInfo
}

func (b *inspectManifestBufferedFile) Close() error { return nil }

func (b *inspectManifestBufferedFile) Stat() (fs.FileInfo, error) {
	return b.info, nil
}

func readInspectManifestBytes(fileIO iceio.IO, manifest iceberg.ManifestFile) (iceio.IO, error) {
	path := manifest.FilePath()
	file, err := fileIO.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open manifest %q for prefetch: %w", path, err)
	}

	info, statErr := file.Stat()
	if statErr != nil {
		// Stat is not required by the manifest decoder. Avoid changing its
		// behavior on filesystems that cannot provide metadata.
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("close manifest %q: %w", path, err)
		}

		return fileIO, nil
	}

	// Do not trust an incorrect manifest-list length to bound memory.
	// Read one byte beyond our limit and fall back to streaming if needed.
	data, readErr := io.ReadAll(io.LimitReader(file, inspectManifestPrefetchMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, fmt.Errorf("prefetch manifest %q: %w", path, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close prefetched manifest %q: %w", path, closeErr)
	}
	if len(data) > inspectManifestPrefetchMaxBytes {
		return fileIO, nil
	}

	return inspectManifestBufferedIO{
		IO: fileIO, path: path, data: data, info: info,
	}, nil
}

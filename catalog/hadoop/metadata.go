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

package hadoop

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

// versionPattern matches Hadoop catalog metadata filenames:
// v1.metadata.json, v42.metadata.json, v1.gz.metadata.json, etc.
var versionPattern = regexp.MustCompile(`^v([0-9]+)(?:\.gz)?\.metadata\.json$`)

// uuidMetadataPattern matches UUID-style metadata filenames produced by
// Java/PyIceberg catalogs: 00000-<uuid>.metadata.json or
// 00000-<uuid>.gz.metadata.json. The numeric sequence has a minimum width
// of 5 digits and the UUID is in canonical 8-4-4-4-12 hex format.
var uuidMetadataPattern = regexp.MustCompile(
	`^([0-9]{5,})-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?:\.gz)?\.metadata\.json$`,
)

// Claims expire aggressively so crashed writers do not permanently block a
// metadata version. Slow live writers may lose a claim and have to retry, but
// the protocol still fails closed instead of publishing duplicate versions.
const metadataClaimStaleAfter = time.Minute

type metadataFile struct {
	location   string
	version    int
	hadoopName bool
	compressed bool
}

func metadataFileFromName(path, name string) (metadataFile, bool) {
	if matches := versionPattern.FindStringSubmatch(name); len(matches) == 2 {
		version, err := strconv.Atoi(matches[1])
		if err != nil || version <= 0 {
			return metadataFile{}, false
		}

		return metadataFile{
			location:   path,
			version:    version,
			hadoopName: true,
			compressed: strings.Contains(name, ".gz.metadata.json"),
		}, true
	}

	if matches := uuidMetadataPattern.FindStringSubmatch(name); len(matches) == 2 {
		version, err := strconv.Atoi(matches[1])
		if err != nil || version < 0 {
			return metadataFile{}, false
		}

		return metadataFile{
			location:   path,
			version:    version,
			compressed: strings.Contains(name, ".gz.metadata.json"),
		}, true
	}

	return metadataFile{}, false
}

func (m metadataFile) betterThan(current metadataFile) bool {
	switch {
	case current.location == "":
		return true
	case m.version != current.version:
		return m.version > current.version
	case m.hadoopName != current.hadoopName:
		// If both naming styles exist for one version, prefer the Hadoop vN
		// file because this catalog writes and conflict-checks that sequence.
		return m.hadoopName
	case m.compressed != current.compressed:
		return !m.compressed
	default:
		// This keeps scans deterministic under map iteration; within one
		// metadata directory it reduces to a stable filename tie-break.
		return m.location > current.location
	}
}

func (c *Catalog) metadataDir(ident table.Identifier) string {
	return joinPath(c.isLocal, c.tableToPath(ident), "metadata")
}

func (c *Catalog) metadataFilePathForCompression(ident table.Identifier, version int, compression string) (string, error) {
	var suffix string
	switch compression {
	case table.MetadataCompressionCodecNone:
		suffix = ".metadata.json"
	case table.MetadataCompressionCodecGzip:
		suffix = ".gz.metadata.json"
	default:
		return "", fmt.Errorf("unsupported write metadata compression codec: %s", compression)
	}

	return joinPath(c.isLocal, c.metadataDir(ident), fmt.Sprintf("v%d%s", version, suffix)), nil
}

func (c *Catalog) metadataVersionClaimPath(ident table.Identifier, version int) string {
	return joinPath(c.isLocal, c.metadataDir(ident), fmt.Sprintf("v%d.metadata.json.claim", version))
}

func (c *Catalog) versionHintPath(ident table.Identifier) string {
	return joinPath(c.isLocal, c.metadataDir(ident), "version-hint.text")
}

// isTableDir reports whether path is a table directory by checking for
// loadable metadata files in its metadata/ subdirectory. It recognizes:
//   - v*.metadata.json (Hadoop catalog format)
//   - <seq>-<uuid>.metadata.json (Java/PyIceberg format)
func isTableDir(filesystem HadoopCatalogFS, isLocal bool, tablePath string) (bool, error) {
	metaDir := joinPath(isLocal, tablePath, "metadata")

	foundMetadata := false
	err := filesystem.WalkDir(metaDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the root itself.
		if path == metaDir {
			return nil
		}

		// Don't descend into subdirectories.
		if d.IsDir() {
			return fs.SkipDir
		}

		if _, ok := metadataFileFromName(path, d.Name()); ok {
			foundMetadata = true

			return fs.SkipAll
		}

		return nil
	})
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}

		return false, err
	}

	return foundMetadata, nil
}

func isTableDirForListing(filesystem HadoopCatalogFS, isLocal bool, tablePath string) (bool, error) {
	isTable, err := isTableDir(filesystem, isLocal, tablePath)
	if err != nil && shouldSuppressPermissionError(err) {
		log.Printf("hadoop catalog: unable to inspect metadata directory %s: %v", joinPath(isLocal, tablePath, "metadata"), err)

		return false, nil
	}

	return isTable, err
}

func (c *Catalog) readVersionHint(ident table.Identifier) int {
	data, err := c.filesystem.ReadFile(c.versionHintPath(ident))
	if err != nil {
		return 0
	}

	ver, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || ver <= 0 {
		return 0
	}

	return ver
}

func (c *Catalog) writeVersionHint(ident table.Identifier, version int) {
	dir := c.metadataDir(ident)
	tempPath := joinPath(c.isLocal, dir, uuid.New().String()+"-version-hint.temp")
	hintPath := c.versionHintPath(ident)

	content := []byte(strconv.Itoa(version))
	if err := c.filesystem.WriteFile(tempPath, content); err != nil {
		log.Printf("hadoop catalog: failed to write version hint temp file: %v", err)

		return
	}

	if err := c.filesystem.Rename(tempPath, hintPath); err != nil {
		log.Printf("hadoop catalog: failed to rename version hint: %v", err)
		_ = c.filesystem.Remove(tempPath)
	}
}

func (c *Catalog) scanMetadataFiles(ident table.Identifier) (map[int]metadataFile, metadataFile, error) {
	dir := c.metadataDir(ident)
	byVersion := map[int]metadataFile{}
	var latest metadataFile

	err := c.filesystem.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == dir {
			return nil
		}
		if d.IsDir() {
			return fs.SkipDir
		}

		file, ok := metadataFileFromName(path, d.Name())
		if !ok {
			return nil
		}

		if file.betterThan(byVersion[file.version]) {
			byVersion[file.version] = file
		}
		if file.betterThan(latest) {
			latest = file
		}

		return nil
	})

	return byVersion, latest, err
}

func (c *Catalog) metadataVersionLocation(ident table.Identifier, version int) (string, bool, error) {
	files, _, err := c.scanMetadataFiles(ident)
	if err != nil {
		return "", false, err
	}

	file, ok := files[version]

	return file.location, ok, nil
}

func (c *Catalog) findMetadataLocation(ident table.Identifier) (string, int, error) {
	_, latest, err := c.scanMetadataFiles(ident)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", 0, fmt.Errorf("hadoop catalog: cannot read metadata directory for %s: %w: %w",
				strings.Join(ident, "."), catalog.ErrNoSuchTable, err)
		}

		return "", 0, fmt.Errorf("hadoop catalog: cannot read metadata directory for %s: %w",
			strings.Join(ident, "."), err)
	}

	if latest.location == "" {
		return "", 0, fmt.Errorf("hadoop catalog: no metadata files found for table %s: %w",
			strings.Join(ident, "."), catalog.ErrNoSuchTable)
	}

	return latest.location, latest.version, nil
}

func (c *Catalog) findVersion(ident table.Identifier) (int, error) {
	_, version, err := c.findMetadataLocation(ident)
	if err != nil {
		return 0, err
	}

	return version, nil
}

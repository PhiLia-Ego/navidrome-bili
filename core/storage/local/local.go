package local

import (
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/djherbis/times"
	"github.com/navidrome/navidrome/adapters/bilibili"
	"github.com/navidrome/navidrome/conf"
	"github.com/navidrome/navidrome/core/storage"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model/metadata"
)

// localStorage implements a Storage that reads the files from the local filesystem and uses registered extractors
// to extract the metadata and tags from the files.
type localStorage struct {
	u            url.URL
	extractor    Extractor
	resolvedPath string
	watching     atomic.Bool
}

func newLocalStorage(u url.URL) storage.Storage {
	newExtractor, ok := extractors[conf.Server.Scanner.Extractor]
	if !ok || newExtractor == nil {
		log.Fatal("Extractor not found", "path", conf.Server.Scanner.Extractor)
	}
	isWindowsPath := filepath.VolumeName(u.Host) != ""
	if u.Scheme == storage.LocalSchemaID && isWindowsPath {
		u.Path = filepath.Join(u.Host, u.Path)
	}
	resolvedPath, err := filepath.EvalSymlinks(u.Path)
	if err != nil {
		log.Warn("Error resolving path", "path", u.Path, "err", err)
		resolvedPath = u.Path
	}
	return &localStorage{u: u, extractor: newExtractor(os.DirFS(u.Path), u.Path), resolvedPath: resolvedPath}
}

func (s *localStorage) FS() (storage.MusicFS, error) {
	path := s.u.Path
	if _, err := os.Stat(path); err != nil { //nolint:gosec
		return nil, fmt.Errorf("%w: %s", err, path)
	}
	return &localFS{FS: os.DirFS(path), extractor: s.extractor, basePath: path}, nil
}

type localFS struct {
	fs.FS
	extractor Extractor
	basePath  string
}

func (lfs *localFS) ReadTags(path ...string) (map[string]metadata.Info, error) {
	res, err := lfs.extractor.Parse(path...)
	if err != nil {
		return nil, err
	}
	for p, v := range res {
		if v.FileInfo == nil {
			info, err := fs.Stat(lfs, p)
			if err != nil {
				return nil, err
			}
			v.FileInfo = localFileInfo{info}
			res[p] = v
		}
	}
	for _, p := range path {
		absPath := filepath.Join(lfs.basePath, p)
		if fallback, ok := bilibili.PlaceholderMetadata(absPath); ok {
			if current, exists := res[p]; exists {
				res[p] = mergeWithFallback(current, *fallback)
				continue
			}
			info, err := fs.Stat(lfs, p)
			if err != nil {
				return nil, err
			}
			fallback.FileInfo = localFileInfo{info}
			res[p] = *fallback
		}
	}
	return res, nil
}

func mergeWithFallback(current, fallback metadata.Info) metadata.Info {
	if current.Tags == nil {
		current.Tags = map[string][]string{}
	}
	for _, k := range []string{"title", "artist", "albumartist", "album"} {
		if len(current.Tags[k]) == 0 && len(fallback.Tags[k]) > 0 {
			current.Tags[k] = fallback.Tags[k]
		}
	}
	if current.AudioProperties.Duration <= 0 {
		current.AudioProperties.Duration = fallback.AudioProperties.Duration
	}
	if current.AudioProperties.BitRate <= 0 {
		current.AudioProperties.BitRate = fallback.AudioProperties.BitRate
	}
	if current.FileInfo == nil {
		current.FileInfo = fallback.FileInfo
	}
	return current
}

// localFileInfo is a wrapper around fs.FileInfo that adds a BirthTime method, to make it compatible
// with metadata.FileInfo
type localFileInfo struct {
	fs.FileInfo
}

func (lfi localFileInfo) BirthTime() time.Time {
	if ts := times.Get(lfi.FileInfo); ts.HasBirthTime() {
		return ts.BirthTime()
	}
	return time.Now()
}

func init() {
	storage.Register(storage.LocalSchemaID, newLocalStorage)
}

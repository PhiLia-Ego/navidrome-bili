package local

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/navidrome/navidrome/conf"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/model/metadata"
)

type bilibiliTestExtractor struct {
	results map[string]metadata.Info
}

func (e bilibiliTestExtractor) Parse(...string) (map[string]metadata.Info, error) {
	return e.results, nil
}
func (e bilibiliTestExtractor) Version() string { return "test" }

func TestReadTagsBilibiliFallback(t *testing.T) {
	t.Cleanup(conf.SnapshotConfig())
	root := t.TempDir()
	conf.Server.Bilibili.Enabled = true
	conf.Server.Bilibili.CacheDir = root
	name := "favorite/track.m4a"
	abs := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, nil, 0600); err != nil {
		t.Fatal(err)
	}
	state := []byte(`{"version":1,"items":{"1:BV1234567890":{"bvid":"BV1234567890","title":"Cached title","artist":"Uploader","folderTitle":"Favorites","duration":123,"sources":[{"codec":"aac","bandwidth":192000,"file":"favorite/track.m4a"}]}}}`)
	if err := os.WriteFile(filepath.Join(root, ".bilibili_state.json"), state, 0600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name     string
		results  map[string]metadata.Info
		title    string
		duration time.Duration
		bitRate  int
	}{
		{name: "missing extractor metadata", title: "Cached title", duration: 123 * time.Second, bitRate: 192},
		{name: "preserves extracted metadata", results: map[string]metadata.Info{
			name: {Tags: model.RawTags{"title": {"Embedded title"}}, AudioProperties: metadata.AudioProperties{Duration: 5 * time.Second, BitRate: 320}},
		}, title: "Embedded title", duration: 5 * time.Second, bitRate: 320},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lfs := &localFS{FS: os.DirFS(root), root: root, extractor: bilibiliTestExtractor{tc.results}}
			got, err := lfs.ReadTags(name)
			if err != nil {
				t.Fatal(err)
			}
			info, ok := got[name]
			if !ok {
				t.Fatal("placeholder was dropped from scan")
			}
			if info.Tags["title"][0] != tc.title || info.Tags["artist"][0] != "Uploader" || info.Tags["album"][0] != "Favorites" {
				t.Fatalf("unexpected tags: %#v", info.Tags)
			}
			if info.AudioProperties.Duration != tc.duration || info.AudioProperties.BitRate != tc.bitRate {
				t.Fatalf("unexpected audio properties: %#v", info.AudioProperties)
			}
			fi, ok := info.FileInfo.(localFileInfo)
			if !ok || fi.path != abs || fi.noBirthTime != &lfs.noBirthTime {
				t.Fatalf("placeholder lost upstream file-info metadata: %#v", info.FileInfo)
			}
			if fi.BirthTime().IsZero() {
				t.Fatal("missing birth time")
			}
		})
	}
	conf.Server.Bilibili.Enabled = false
	lfs := &localFS{FS: os.DirFS(root), root: root, extractor: bilibiliTestExtractor{}}
	got, err := lfs.ReadTags(name)
	if err != nil || len(got) != 0 {
		t.Fatalf("disabled Bilibili injected metadata: %#v, %v", got, err)
	}
}

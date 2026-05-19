package bilibili

import (
	"net/url"
	"os"

	"github.com/navidrome/navidrome/conf"
	"github.com/navidrome/navidrome/core/storage"
	"github.com/navidrome/navidrome/log"
)

const BilibiliSchemaID string = "bilibili"

// Accepted URL: bilibili://video/<BV_ID>  TODO: Add old AV_ID support
// 				 bilibili://audio/<AU_ID>
// Parsed URL: https://real.cdn.path/to/audios

type bilibibiliStorage struct {
	u            url.URL
	extractor    Extractor
}

func (s *bilibibiliStorage) FS() (storage.MusicFS, error) {
	return nil, nil
}

func newBilibiliStorage(u url.URL) storage.Storage {
	newExtractor, ok := extractors[conf.Server.Scanner.Extractor]
	if !ok || newExtractor == nil {
		log.Fatal("Extractor not found", "path", conf.Server.Scanner.Extractor)
	}
	if !conf.Server.Bilibili.Enabled {
		return nil
	}
	return &bilibibiliStorage{u: u, extractor: newExtractor(os.DirFS(u.Path), u.Path)}
}

func init() {
	storage.Register(BilibiliSchemaID, newBilibiliStorage)
}

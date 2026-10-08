package bilibili

import (
	"errors"
	"net/url"

	"github.com/navidrome/navidrome/core/storage"
)

const BilibiliSchemaID string = "bilibili"

// Direct bilibili://video/<BV_ID> and bilibili://audio/<AU_ID> storage is not
// implemented. Favorite-folder syncing uses adapters/bilibili and local storage.
type bilibiliStorage struct{}

func (s *bilibiliStorage) FS() (storage.MusicFS, error) {
	return nil, errors.New("direct bilibili URLs are not supported: configure Bilibili.FavoriteFolders and scan the local cache")
}

func newBilibiliStorage(_ url.URL) storage.Storage {
	return &bilibiliStorage{}
}

func init() {
	storage.Register(BilibiliSchemaID, newBilibiliStorage)
}

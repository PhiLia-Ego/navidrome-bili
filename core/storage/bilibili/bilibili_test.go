package bilibili

import (
	"testing"

	"github.com/navidrome/navidrome/conf"
	"github.com/navidrome/navidrome/core/storage"
)

func TestDirectURLReturnsExplicitError(t *testing.T) {
	t.Cleanup(conf.SnapshotConfig())
	for _, enabled := range []bool{false, true} {
		conf.Server.Bilibili.Enabled = enabled
		for _, uri := range []string{"bilibili://video/BV1234567890", "bilibili://audio/12345"} {
			s, err := storage.For(uri)
			if err != nil || s == nil {
				t.Fatalf("storage.For(%q), enabled=%v: storage=%v, error=%v", uri, enabled, s, err)
			}
			fs, err := s.FS()
			if err == nil || fs != nil {
				t.Fatalf("unimplemented storage returned success: fs=%v, error=%v", fs, err)
			}
		}
	}
}

package bilibili

import (
	"testing"
)

func TestParseOneTarget(t *testing.T) {
	t.Run("plain number", func(t *testing.T) {
		got, err := parseOneTarget("54613383")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.FID != 54613383 || got.MediaID != 54613383 {
			t.Fatalf("unexpected target: %#v", got)
		}
	})

	t.Run("space link", func(t *testing.T) {
		raw := "https://space.bilibili.com/123456/favlist?fid=54613383"
		got, err := parseOneTarget(raw)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.FID != 54613383 {
			t.Fatalf("unexpected fid: %#v", got)
		}
		if got.UpMid != 123456 {
			t.Fatalf("unexpected up_mid: %#v", got)
		}
		if got.MediaID != 54613383 {
			t.Fatalf("unexpected media_id: %#v", got)
		}
	})
}

func TestSelectBestSources(t *testing.T) {
	in := []audioSource{
		{ID: 30216, Bandwidth: 64000, Codecs: "mp4a.40.2", BaseURL: "a"},
		{ID: 30280, Bandwidth: 192000, Codecs: "mp4a.40.2", BaseURL: "b"},
		{ID: 30251, Bandwidth: 999000, Codecs: "fLaC", BaseURL: "c"},
		{ID: 30250, Bandwidth: 512000, Codecs: "ec-3", BaseURL: "d"},
	}
	out := selectBestSources(in)
	if len(out) != 3 {
		t.Fatalf("expected 3 selected sources, got %d", len(out))
	}
	if codecFamily(out[0]) != "aac" || out[0].ID != 30280 {
		t.Fatalf("unexpected aac selection: %#v", out[0])
	}
	if codecFamily(out[1]) != "flac" || out[1].ID != 30251 {
		t.Fatalf("unexpected flac selection: %#v", out[1])
	}
	if codecFamily(out[2]) != "lc3" || out[2].ID != 30250 {
		t.Fatalf("unexpected lc3 selection: %#v", out[2])
	}
}

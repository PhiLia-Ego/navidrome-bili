package bilibili

import (
	"context"
	"database/sql"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/navidrome/navidrome/conf"
	"github.com/navidrome/navidrome/db"
	"github.com/navidrome/navidrome/utils/singleton"
	"github.com/pressly/goose/v3"
)

// Exercise the fork's last schema before this upstream sync, with a synthetic
// cached track, then verify favorite timestamps still work after all migrations.
func TestFavoriteTimeAfterUpstreamMigration(t *testing.T) {
	t.Cleanup(conf.SnapshotConfig())
	ctx := context.Background()
	cacheDir := t.TempDir()
	conf.Server.DbPath = ":memory:"
	conf.Server.MusicFolder = cacheDir
	conf.Server.Bilibili.Enabled = true
	conf.Server.Bilibili.CacheDir = cacheDir

	singleton.DeleteInstance[*sql.DB]()
	database := db.Db()
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		_ = database.Close()
		singleton.DeleteInstance[*sql.DB]()
		goose.SetBaseFS(nil)
	})
	if _, err := database.ExecContext(ctx, "PRAGMA foreign_keys=off"); err != nil {
		t.Fatal(err)
	}
	goose.SetBaseFS(nil)
	if err := goose.SetDialect("sqlite3"); err != nil {
		t.Fatal(err)
	}
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate migration fixtures")
	}
	migrationsDir := filepath.Join(filepath.Dir(filename), "../../db/migrations")
	const forkSchemaVersion int64 = 20260513173954
	if err := goose.UpToContext(ctx, database, migrationsDir, forkSchemaVersion); err != nil {
		t.Fatal(err)
	}

	const trackPath = "favorite/track.m4a"
	const trackID = "0123456789abcdef0123456789abcdef"
	if _, err := database.ExecContext(ctx,
		"INSERT INTO media_file (id, library_id, path, title, created_at) VALUES (?, 1, ?, ?, ?)",
		trackID, trackPath, "Cached title", time.Unix(1700000000, 0).UTC()); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpContext(ctx, database, migrationsDir); err != nil {
		t.Fatal(err)
	}
	var integrity string
	if err := database.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatalf("migrated database integrity: %q, %v", integrity, err)
	}

	const favTime int64 = 1701000000
	state := &syncState{Items: map[string]stateItem{
		"1:BV1234567890": {FID: 1, BVID: "BV1234567890", FavTime: favTime, Sources: []savedSource{{File: trackPath}}},
	}}
	if err := saveState(filepath.Join(cacheDir, stateFileName), state); err != nil {
		t.Fatal(err)
	}
	updated, err := SyncDateAddedFromFavTime(ctx)
	if err != nil || updated != 1 {
		t.Fatalf("favorite-time update after migration: updated=%d, error=%v", updated, err)
	}
	var createdAt time.Time
	if err := database.QueryRowContext(ctx, "SELECT created_at FROM media_file WHERE library_id=1 AND path=?", trackPath).Scan(&createdAt); err != nil {
		t.Fatal(err)
	}
	if createdAt.Unix() != favTime {
		t.Fatalf("favorite timestamp changed: got %v, want %d", createdAt, favTime)
	}
	updated, err = SyncDateAddedFromFavTime(ctx)
	if err != nil || updated != 0 {
		t.Fatalf("favorite-time update should be idempotent: updated=%d, error=%v", updated, err)
	}
}

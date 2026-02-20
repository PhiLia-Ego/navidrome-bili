package bilibili

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/navidrome/navidrome/conf"
	"github.com/navidrome/navidrome/db"
	"github.com/navidrome/navidrome/log"
	"github.com/navidrome/navidrome/model"
	"github.com/navidrome/navidrome/model/metadata"
)

const (
	apiBaseURL       = "https://api.bilibili.com"
	defaultPageSize  = 20
	stateFileName    = ".bilibili_state.json"
	defaultUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

type SyncResult struct {
	Changed bool
	Added   int
	Updated int
	Removed int
	Skipped int
}

type Syncer interface {
	Sync(ctx context.Context) (SyncResult, error)
}

func NewSyncer() Syncer {
	timeout := conf.Server.Bilibili.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &syncer{
		client: &http.Client{Timeout: timeout},
	}
}

type syncer struct {
	client *http.Client
}

type syncState struct {
	Version int                  `json:"version"`
	Items   map[string]stateItem `json:"items"`
}

type stateItem struct {
	MediaID     int64         `json:"mediaId"`
	FID         int64         `json:"fid"`
	FavTime     int64         `json:"favTime"`
	BVID        string        `json:"bvid"`
	Title       string        `json:"title"`
	Artist      string        `json:"artist"`
	FolderTitle string        `json:"folderTitle"`
	Duration    int           `json:"duration"`
	CID         int64         `json:"cid"`
	Sources     []savedSource `json:"sources"`
}

type savedSource struct {
	Codec      string `json:"codec"`
	ID         int    `json:"id"`
	Bandwidth  int    `json:"bandwidth"`
	Codecs     string `json:"codecs"`
	SourceHash string `json:"sourceHash"`
	File       string `json:"file"`
}

type favoriteTarget struct {
	MediaID int64
	FID     int64
	UpMid   int64
}

//nolint:gocyclo // Sync orchestrates multiple API and cache steps in one flow.
func (s *syncer) Sync(ctx context.Context) (SyncResult, error) {
	if !conf.Server.Bilibili.Enabled {
		return SyncResult{}, nil
	}
	if strings.TrimSpace(conf.Server.Bilibili.Cookies) == "" {
		return SyncResult{}, errors.New("bilibili enabled but Bilibili.Cookies is empty")
	}
	if strings.TrimSpace(conf.Server.Bilibili.FavoriteFolders) == "" {
		return SyncResult{}, errors.New("bilibili enabled but Bilibili.FavoriteFolders is empty")
	}
	cacheDir := resolveCacheDir()
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return SyncResult{}, fmt.Errorf("creating bilibili cache dir: %w", err)
	}

	statePath := filepath.Join(cacheDir, stateFileName)
	state, err := loadState(statePath)
	if err != nil {
		return SyncResult{}, err
	}

	targets, err := parseFavoriteTargets(conf.Server.Bilibili.FavoriteFolders)
	if err != nil {
		return SyncResult{}, err
	}

	bc := &client{
		httpClient: s.client,
		cookies:    conf.Server.Bilibili.Cookies,
	}

	// The configured fid is already the media_id accepted by /x/v3/fav/resource/list.
	for i := range targets {
		if targets[i].MediaID == 0 {
			targets[i].MediaID = targets[i].FID
		}
	}

	newItems := map[string]stateItem{}
	var result SyncResult

	for _, t := range targets {
		folderTitle, medias, err := bc.listFavorites(ctx, t.MediaID)
		if err != nil {
			return result, err
		}

		for _, media := range medias {
			if media.Type != 2 || media.Attr != 0 || media.BVID == "" {
				continue
			}

			itemKey := makeItemKey(t.FID, media.BVID)
			prev, hasPrev := state.Items[itemKey]
			if hasPrev && prev.FavTime == media.FavTime && allSourceFilesExist(cacheDir, prev.Sources) {
				newItems[itemKey] = prev
				result.Skipped++
				continue
			}

			video, err := bc.getVideoInfo(ctx, media.BVID)
			if err != nil {
				log.Warn(ctx, "Bilibili sync: failed to fetch video info, skipping media", "bvid", media.BVID, err)
				continue
			}
			cid := video.CID
			if cid == 0 && len(video.Pages) > 0 {
				cid = video.Pages[0].CID
			}
			if cid == 0 {
				log.Warn(ctx, "Bilibili sync: no CID found, skipping media", "bvid", media.BVID)
				continue
			}

			sources, err := bc.getAudioSources(ctx, media.BVID, cid)
			if err != nil {
				log.Warn(ctx, "Bilibili sync: failed to fetch audio sources, skipping media", "bvid", media.BVID, "cid", cid, err)
				continue
			}
			selected := selectBestSources(sources)
			if len(selected) == 0 {
				log.Warn(ctx, "Bilibili sync: no selectable audio sources", "bvid", media.BVID, "cid", cid)
				continue
			}

			title := media.Title
			if title == "" {
				title = video.Title
			}
			artist := media.Upper.Name
			if artist == "" {
				artist = video.Owner.Name
			}
			fidDir := filepath.Join(cacheDir, fmt.Sprintf("fid_%d", t.FID))
			if err := os.MkdirAll(fidDir, 0755); err != nil {
				return result, fmt.Errorf("creating fid cache dir: %w", err)
			}

			item := stateItem{
				MediaID:     t.MediaID,
				FID:         t.FID,
				FavTime:     media.FavTime,
				BVID:        media.BVID,
				Title:       title,
				Artist:      artist,
				FolderTitle: folderTitle,
				Duration:    media.Duration,
				CID:         cid,
			}
			for _, src := range selected {
				targetFile := buildTargetFilename(fidDir, title, artist, media.BVID, src)
				sourceHash := hashString(fmt.Sprintf("%d|%d|%s", src.ID, src.Bandwidth, src.Codecs))
				needsPlaceholder := true
				invalidateCached := false
				if hasPrev {
					for _, old := range prev.Sources {
						if old.Codec == codecFamily(src) {
							if old.SourceHash == sourceHash {
								targetFile = filepath.Join(cacheDir, old.File)
								if _, err := os.Stat(targetFile); err == nil {
									needsPlaceholder = false
								}
							} else {
								targetFile = filepath.Join(cacheDir, old.File)
								invalidateCached = true
							}
							break
						}
					}
				}
				if needsPlaceholder || invalidateCached {
					if err := ensurePlaceholder(targetFile, invalidateCached); err != nil {
						log.Warn(ctx, "Bilibili sync: failed creating placeholder", "path", targetFile, err)
						continue
					}
					if hasPrev {
						result.Updated++
					} else {
						result.Added++
					}
					result.Changed = true
				} else {
					result.Skipped++
				}
				item.Sources = append(item.Sources, savedSource{
					Codec:      codecFamily(src),
					ID:         src.ID,
					Bandwidth:  src.Bandwidth,
					Codecs:     src.Codecs,
					SourceHash: sourceHash,
					File:       relToCache(cacheDir, targetFile),
				})
			}
			if len(item.Sources) > 0 {
				newItems[itemKey] = item
			}
		}
	}

	for key, old := range state.Items {
		if _, ok := newItems[key]; ok {
			continue
		}
		for _, src := range old.Sources {
			_ = os.Remove(filepath.Join(cacheDir, src.File))
		}
		result.Removed++
		result.Changed = true
	}

	state.Items = newItems
	if err := saveState(statePath, state); err != nil {
		return result, err
	}
	return result, nil
}

func resolveCacheDir() string {
	cacheDir := conf.Server.Bilibili.CacheDir
	if cacheDir == "" {
		return filepath.Join(conf.Server.DataFolder, "bilibili-cache")
	}
	return cacheDir
}

func resolveCacheDirAbs() string {
	cacheDir := resolveCacheDir()
	abs, err := filepath.Abs(cacheDir)
	if err != nil {
		return cacheDir
	}
	return abs
}

func ensurePlaceholder(targetFile string, truncate bool) error {
	if err := os.MkdirAll(filepath.Dir(targetFile), 0755); err != nil {
		return err
	}
	if truncate {
		return os.WriteFile(targetFile, nil, 0600)
	}
	if _, err := os.Stat(targetFile); err == nil {
		return nil
	}
	f, err := os.Create(targetFile)
	if err != nil {
		return err
	}
	return f.Close()
}

// EnsureCached downloads a real audio file only when the placeholder is requested by playback.
func EnsureCached(ctx context.Context, absPath string) error {
	if !conf.Server.Bilibili.Enabled {
		return nil
	}
	absPath = normalizeAbsPath(absPath)

	info, err := os.Stat(absPath)
	if err == nil && info.Size() > 0 {
		return nil
	}
	rootDir, rel, state, err := resolveStateForPath(absPath)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	item, src, ok := findSourceByFile(state, rel)
	if !ok {
		item, src, ok = findSourceByNameFallback(state, rel)
		if !ok {
			log.Info(ctx, "Bilibili on-demand cache: source not found in state", "path", absPath, "rel", rel)
			return nil
		}
	}

	timeout := conf.Server.Bilibili.RequestTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	bc := &client{
		httpClient: &http.Client{Timeout: timeout},
		cookies:    conf.Server.Bilibili.Cookies,
	}

	allSources, err := bc.getAudioSources(ctx, item.BVID, item.CID)
	if err != nil {
		return err
	}
	selected := selectBestSources(allSources)
	target, found := pickByCodec(selected, src.Codec)
	if !found {
		return fmt.Errorf("no bilibili source available for codec family %s", src.Codec)
	}

	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		return err
	}
	log.Info(ctx, "Bilibili on-demand cache: downloading source", "path", absPath, "codec", src.Codec, "bvid", item.BVID, "stateRoot", rootDir)
	return bc.downloadFile(ctx, target.BaseURL, absPath)
}

func PlaceholderMetadata(absPath string) (*metadata.Info, bool) {
	if !conf.Server.Bilibili.Enabled {
		return nil, false
	}
	absPath = normalizeAbsPath(absPath)
	_, rel, state, err := resolveStateForPath(absPath)
	if err != nil {
		return nil, false
	}
	if state == nil {
		return nil, false
	}
	item, src, ok := findSourceByFile(state, rel)
	if !ok {
		return nil, false
	}
	album := item.FolderTitle
	if album == "" {
		album = "Bilibili"
	}
	res := &metadata.Info{
		Tags: model.RawTags{
			"title":       {item.Title},
			"artist":      {item.Artist},
			"albumartist": {item.Artist},
			"album":       {album},
		},
		AudioProperties: metadata.AudioProperties{
			Duration: time.Duration(item.Duration) * time.Second,
			BitRate:  src.Bandwidth / 1000,
		},
	}
	return res, true
}

func findSourceByFile(state *syncState, rel string) (stateItem, savedSource, bool) {
	rel = filepath.ToSlash(rel)
	for _, item := range state.Items {
		for _, src := range item.Sources {
			if normPath(filepath.ToSlash(src.File)) == normPath(rel) {
				return item, src, true
			}
		}
	}
	return stateItem{}, savedSource{}, false
}

// Fallback matcher for filesystems that normalize unicode differently (common on macOS).
// It matches by BVID + codec family extracted from filename.
func findSourceByNameFallback(state *syncState, rel string) (stateItem, savedSource, bool) {
	base := filepath.Base(rel)
	codec := ""
	switch {
	case strings.Contains(strings.ToLower(base), "[flac]"):
		codec = "flac"
	case strings.Contains(strings.ToLower(base), "[lc3]"):
		codec = "lc3"
	case strings.Contains(strings.ToLower(base), "[aac]"):
		codec = "aac"
	}
	bvid := extractBVID(base)

	for _, item := range state.Items {
		if bvid != "" && item.BVID != "" && !strings.EqualFold(item.BVID, bvid) {
			continue
		}
		for _, src := range item.Sources {
			if codec != "" && src.Codec != codec {
				continue
			}
			return item, src, true
		}
	}
	return stateItem{}, savedSource{}, false
}

func extractBVID(name string) string {
	i := strings.Index(name, "[BV")
	if i < 0 {
		return ""
	}
	rest := name[i+1:]
	j := strings.Index(rest, "]")
	if j <= 0 {
		return ""
	}
	candidate := rest[:j]
	if strings.HasPrefix(candidate, "BV") && utf8.ValidString(candidate) {
		return candidate
	}
	return ""
}

func normPath(p string) string {
	return strings.ToLower(filepath.Clean(strings.TrimSpace(p)))
}

func pickByCodec(sources []audioSource, codec string) (audioSource, bool) {
	for _, src := range sources {
		if codecFamily(src) == codec {
			return src, true
		}
	}
	if len(sources) == 0 {
		return audioSource{}, false
	}
	return sources[0], true
}

func normalizeAbsPath(p string) string {
	p = filepath.Clean(p)
	abs, err := filepath.Abs(p)
	if err != nil {
		return p
	}
	return abs
}

func resolveStateForPath(absPath string) (rootDir string, rel string, st *syncState, err error) {
	absPath = normalizeAbsPath(absPath)
	checked := map[string]struct{}{}
	candidates := []string{filepath.Join(resolveCacheDirAbs(), stateFileName)}
	for dir := filepath.Dir(absPath); ; dir = filepath.Dir(dir) {
		candidates = append(candidates, filepath.Join(dir, stateFileName))
		next := filepath.Dir(dir)
		if next == dir {
			break
		}
	}
	for _, candidate := range candidates {
		candidate = filepath.Clean(candidate)
		if _, ok := checked[candidate]; ok {
			continue
		}
		checked[candidate] = struct{}{}
		if _, statErr := os.Stat(candidate); statErr != nil {
			continue
		}
		st, err = loadState(candidate)
		if err != nil {
			return "", "", nil, err
		}
		root := filepath.Dir(candidate)
		relPath, relErr := filepath.Rel(root, absPath)
		if relErr != nil || strings.HasPrefix(relPath, "..") {
			continue
		}
		return root, relPath, st, nil
	}
	return "", "", nil, nil
}

func makeItemKey(fid int64, bvid string) string {
	return fmt.Sprintf("%d:%s", fid, bvid)
}

func relToCache(cacheDir, absPath string) string {
	rel, err := filepath.Rel(cacheDir, absPath)
	if err != nil {
		return absPath
	}
	return rel
}

func loadState(path string) (*syncState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &syncState{Version: 1, Items: map[string]stateItem{}}, nil
		}
		return nil, fmt.Errorf("reading bilibili state: %w", err)
	}
	var st syncState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("parsing bilibili state: %w", err)
	}
	if st.Items == nil {
		st.Items = map[string]stateItem{}
	}
	return &st, nil
}

func saveState(path string, st *syncState) error {
	st.Version = 1
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("serializing bilibili state: %w", err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("writing bilibili state: %w", err)
	}
	return nil
}

// SyncDateAddedFromFavTime maps bilibili favorite time to media_file.created_at
// so "Date Added" follows the favorite order.
func SyncDateAddedFromFavTime(ctx context.Context) (int, error) {
	if !conf.Server.Bilibili.Enabled {
		return 0, nil
	}
	cacheDir := resolveCacheDirAbs()
	statePath := filepath.Join(cacheDir, stateFileName)
	st, err := loadState(statePath)
	if err != nil {
		return 0, err
	}
	assignments, err := buildFavTimeAssignments(ctx, cacheDir, st)
	if err != nil {
		return 0, err
	}
	if len(assignments) == 0 {
		return 0, nil
	}

	tx, err := db.Db().BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, `
		UPDATE media_file
		SET created_at = ?
		WHERE library_id = ?
		  AND (path = ? OR path = ?)
		  AND (created_at IS NULL OR created_at <> ?)
	`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	updated := 0
	for _, a := range assignments {
		ts := time.Unix(a.FavTime, 0).UTC()
		res, err := stmt.ExecContext(ctx, ts, a.LibraryID, a.Path, filepath.ToSlash(a.Path), ts)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		updated += int(n)
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return updated, nil
}

type libraryRow struct {
	ID   int
	Path string
	Abs  string
}

type favTimeAssignment struct {
	LibraryID int
	Path      string
	FavTime   int64
}

func buildFavTimeAssignments(ctx context.Context, cacheDir string, st *syncState) ([]favTimeAssignment, error) {
	libraries, err := loadLibraries(ctx)
	if err != nil {
		return nil, err
	}
	if len(libraries) == 0 || st == nil || len(st.Items) == 0 {
		return nil, nil
	}

	byKey := map[string]favTimeAssignment{}
	for _, item := range st.Items {
		if item.FavTime <= 0 {
			continue
		}
		for _, src := range item.Sources {
			absFile := normalizeAbsPath(filepath.Join(cacheDir, src.File))
			lib, rel, ok := matchLibrary(libraries, absFile)
			if !ok {
				continue
			}
			rel = filepath.Clean(rel)
			key := fmt.Sprintf("%d:%s", lib.ID, filepath.ToSlash(rel))
			prev, exists := byKey[key]
			if !exists || item.FavTime > prev.FavTime {
				byKey[key] = favTimeAssignment{
					LibraryID: lib.ID,
					Path:      rel,
					FavTime:   item.FavTime,
				}
			}
		}
	}

	out := make([]favTimeAssignment, 0, len(byKey))
	for _, a := range byKey {
		out = append(out, a)
	}
	return out, nil
}

func loadLibraries(ctx context.Context) ([]libraryRow, error) {
	rows, err := db.Db().QueryContext(ctx, `SELECT id, path FROM library`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var libs []libraryRow
	for rows.Next() {
		var lib libraryRow
		if err := rows.Scan(&lib.ID, &lib.Path); err != nil {
			return nil, err
		}
		lib.Abs = normalizeAbsPath(lib.Path)
		libs = append(libs, lib)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return libs, nil
}

func matchLibrary(libs []libraryRow, absFile string) (libraryRow, string, bool) {
	var selected libraryRow
	selectedRel := ""
	selectedLen := -1
	for _, lib := range libs {
		rel, err := filepath.Rel(lib.Abs, absFile)
		if err != nil {
			continue
		}
		if rel == "." || strings.HasPrefix(rel, "..") {
			continue
		}
		if len(lib.Abs) > selectedLen {
			selected = lib
			selectedRel = rel
			selectedLen = len(lib.Abs)
		}
	}
	if selectedLen < 0 {
		return libraryRow{}, "", false
	}
	return selected, selectedRel, true
}

func allSourceFilesExist(cacheDir string, sources []savedSource) bool {
	if len(sources) == 0 {
		return false
	}
	for _, src := range sources {
		if _, err := os.Stat(filepath.Join(cacheDir, src.File)); err != nil {
			return false
		}
	}
	return true
}

func parseFavoriteTargets(raw string) ([]favoriteTarget, error) {
	parts := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\t' || r == ' '
	})
	var targets []favoriteTarget
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		target, err := parseOneTarget(part)
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%d:%d:%d", target.MediaID, target.FID, target.UpMid)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return nil, errors.New("no valid bilibili favorites configured")
	}
	return targets, nil
}

func parseOneTarget(raw string) (favoriteTarget, error) {
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return favoriteTarget{FID: n, MediaID: n}, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return favoriteTarget{}, fmt.Errorf("invalid bilibili favorite: %s", raw)
	}
	q := u.Query()
	fid, _ := strconv.ParseInt(q.Get("fid"), 10, 64)
	if fid == 0 {
		// Allow "media_id=xxxx"
		fid, _ = strconv.ParseInt(q.Get("media_id"), 10, 64)
	}
	if fid == 0 {
		return favoriteTarget{}, fmt.Errorf("invalid bilibili favorite, missing fid: %s", raw)
	}
	upMid := int64(0)
	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if strings.Contains(strings.ToLower(u.Host), "space.bilibili.com") && len(segments) > 0 {
		upMid, _ = strconv.ParseInt(segments[0], 10, 64)
	}
	for i, s := range segments {
		if s == "space" && i+1 < len(segments) {
			upMid, _ = strconv.ParseInt(segments[i+1], 10, 64)
			break
		}
	}
	return favoriteTarget{FID: fid, MediaID: fid, UpMid: upMid}, nil
}

type client struct {
	httpClient *http.Client
	cookies    string
}

type apiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *client) doGET(ctx context.Context, apiPath string, params url.Values, out any) error {
	endpoint, err := buildAPIEndpoint(apiPath, params)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Referer", "https://www.bilibili.com/")
	req.Header.Set("Cookie", c.cookies)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d from bilibili api: %s", resp.StatusCode, apiPath)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("invalid bilibili response from %s: %w", apiPath, err)
	}
	return nil
}

type listResp struct {
	apiError
	Data struct {
		Info struct {
			FID   int64  `json:"fid"`
			Title string `json:"title"`
		} `json:"info"`
		Medias  []favoriteMedia `json:"medias"`
		HasMore bool            `json:"has_more"`
	} `json:"data"`
}

type favoriteMedia struct {
	ID       int64  `json:"id"`
	Type     int    `json:"type"`
	Title    string `json:"title"`
	Attr     int    `json:"attr"`
	Duration int    `json:"duration"`
	FavTime  int64  `json:"fav_time"`
	BVID     string `json:"bvid"`
	Upper    struct {
		Name string `json:"name"`
	} `json:"upper"`
}

func (c *client) listFavorites(ctx context.Context, mediaID int64) (string, []favoriteMedia, error) {
	var all []favoriteMedia
	folderTitle := ""
	pn := 1
	for {
		var resp listResp
		params := url.Values{}
		params.Set("media_id", strconv.FormatInt(mediaID, 10))
		params.Set("pn", strconv.Itoa(pn))
		params.Set("ps", strconv.Itoa(defaultPageSize))
		params.Set("platform", "web")
		if err := c.doGET(ctx, "/x/v3/fav/resource/list", params, &resp); err != nil {
			return "", nil, err
		}
		if resp.Code != 0 {
			return "", nil, fmt.Errorf("bilibili list favorites failed: media_id=%d code=%d message=%s", mediaID, resp.Code, resp.Message)
		}
		if folderTitle == "" {
			folderTitle = resp.Data.Info.Title
		}
		all = append(all, resp.Data.Medias...)
		if !resp.Data.HasMore {
			break
		}
		pn++
	}
	return folderTitle, all, nil
}

type videoInfoResp struct {
	apiError
	Data videoInfo `json:"data"`
}

type videoInfo struct {
	Title string `json:"title"`
	CID   int64  `json:"cid"`
	Owner struct {
		Name string `json:"name"`
	} `json:"owner"`
	Pages []struct {
		CID int64 `json:"cid"`
	} `json:"pages"`
}

func (c *client) getVideoInfo(ctx context.Context, bvid string) (*videoInfo, error) {
	var resp videoInfoResp
	params := url.Values{}
	params.Set("bvid", bvid)
	if err := c.doGET(ctx, "/x/web-interface/view", params, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("bilibili view failed: bvid=%s code=%d message=%s", bvid, resp.Code, resp.Message)
	}
	return &resp.Data, nil
}

type navResp struct {
	apiError
	Data struct {
		WBIImg struct {
			ImgURL string `json:"img_url"`
			SubURL string `json:"sub_url"`
		} `json:"wbi_img"`
	} `json:"data"`
}

func (c *client) getWBIKeys(ctx context.Context) (string, string, error) {
	var resp navResp
	if err := c.doGET(ctx, "/x/web-interface/nav", nil, &resp); err != nil {
		return "", "", err
	}
	if resp.Code != 0 {
		return "", "", fmt.Errorf("bilibili nav failed: code=%d message=%s", resp.Code, resp.Message)
	}
	img := keyFromURL(resp.Data.WBIImg.ImgURL)
	sub := keyFromURL(resp.Data.WBIImg.SubURL)
	if img == "" || sub == "" {
		return "", "", errors.New("invalid wbi keys from nav")
	}
	return img, sub, nil
}

func keyFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	name := filepath.Base(raw)
	return strings.TrimSuffix(name, filepath.Ext(name))
}

type playURLResp struct {
	apiError
	Data struct {
		Dash struct {
			Audio []audioSource `json:"audio"`
			Flac  struct {
				Audio *audioSource `json:"audio"`
			} `json:"flac"`
			Dolby struct {
				Audio []audioSource `json:"audio"`
			} `json:"dolby"`
		} `json:"dash"`
	} `json:"data"`
}

type audioSource struct {
	ID        int      `json:"id"`
	BaseURL   string   `json:"base_url"`
	BackupURL []string `json:"backup_url"`
	Bandwidth int      `json:"bandwidth"`
	Codecs    string   `json:"codecs"`
}

func (c *client) getAudioSources(ctx context.Context, bvid string, cid int64) ([]audioSource, error) {
	img, sub, err := c.getWBIKeys(ctx)
	if err != nil {
		return nil, err
	}

	params := map[string]string{
		"bvid":  bvid,
		"cid":   strconv.FormatInt(cid, 10),
		"fnval": "4048", // all available DASH streams
		"fnver": "0",
		"qn":    "127",
		"fourk": "1",
	}
	signed := signWBI(params, img, sub)

	var resp playURLResp
	if err := c.doGET(ctx, "/x/player/wbi/playurl", signed, &resp); err != nil {
		return nil, err
	}
	if resp.Code != 0 {
		return nil, fmt.Errorf("bilibili playurl failed: bvid=%s cid=%d code=%d message=%s", bvid, cid, resp.Code, resp.Message)
	}
	var out []audioSource
	out = append(out, resp.Data.Dash.Audio...)
	if resp.Data.Dash.Flac.Audio != nil {
		out = append(out, *resp.Data.Dash.Flac.Audio)
	}
	out = append(out, resp.Data.Dash.Dolby.Audio...)
	return out, nil
}

func (c *client) downloadFile(ctx context.Context, sourceURL, targetPath string) error {
	if err := validateDownloadURL(sourceURL); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Referer", "https://www.bilibili.com/")
	req.Header.Set("Cookie", c.cookies)
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("http %d while downloading source", resp.StatusCode)
	}

	tmp := targetPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := io.Copy(f, resp.Body); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, targetPath); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func buildAPIEndpoint(apiPath string, params url.Values) (string, error) {
	if !strings.HasPrefix(apiPath, "/") {
		return "", fmt.Errorf("invalid bilibili api path: %s", apiPath)
	}
	endpoint := apiBaseURL + apiPath
	if len(params) > 0 {
		endpoint += "?" + params.Encode()
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(u.Scheme, "https") || !strings.EqualFold(u.Hostname(), "api.bilibili.com") {
		return "", fmt.Errorf("refusing non-bilibili api endpoint: %s", endpoint)
	}
	return endpoint, nil
}

func validateDownloadURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if !strings.EqualFold(u.Scheme, "https") {
		return fmt.Errorf("refusing non-https source url")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return errors.New("empty source host")
	}
	allowedSuffixes := []string{
		".bilivideo.com",
		".hdslb.com",
		".mcdn.bilivideo.cn",
	}
	for _, suffix := range allowedSuffixes {
		if strings.HasSuffix(host, suffix) {
			return nil
		}
	}
	return fmt.Errorf("refusing untrusted source host: %s", host)
}

func buildTargetFilename(baseDir, title, artist, bvid string, src audioSource) string {
	ext := "m4a"
	switch codecFamily(src) {
	case "flac":
		ext = "flac"
	case "lc3":
		ext = "m4a"
	}
	name := sanitizeFilename(fmt.Sprintf("%s - %s [%s] [%s].%s",
		compactTitle(title), compactTitle(artist), bvid, codecFamily(src), ext))
	return filepath.Join(baseDir, name)
}

func compactTitle(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	return s
}

func sanitizeFilename(s string) string {
	replacer := strings.NewReplacer(
		"/", "_", "\\", "_", ":", "_", "*", "_", "?", "_", "\"", "_",
		"<", "_", ">", "_", "|", "_", "\n", " ", "\r", " ", "\t", " ",
	)
	s = replacer.Replace(s)
	s = strings.TrimSpace(s)
	if s == "" {
		return "untitled"
	}
	return s
}

func codecFamily(src audioSource) string {
	codec := strings.ToLower(src.Codecs)
	switch {
	case src.ID == 30251 || strings.Contains(codec, "flac"):
		return "flac"
	case src.ID == 30250 || strings.Contains(codec, "ec-3") || strings.Contains(codec, "eac-3") || strings.Contains(codec, "lc3"):
		return "lc3"
	default:
		return "aac"
	}
}

func selectBestSources(sources []audioSource) []audioSource {
	best := map[string]audioSource{}
	for _, s := range sources {
		if s.BaseURL == "" {
			continue
		}
		family := codecFamily(s)
		cur, ok := best[family]
		if !ok || s.Bandwidth > cur.Bandwidth {
			best[family] = s
		}
	}
	out := make([]audioSource, 0, len(best))
	keys := []string{"aac", "flac", "lc3"}
	for _, k := range keys {
		if src, ok := best[k]; ok {
			out = append(out, src)
		}
	}
	// Keep deterministic order for unknown families as well.
	if len(out) != len(best) {
		for family, src := range best {
			if slices.Contains(keys, family) {
				continue
			}
			out = append(out, src)
		}
	}
	return out
}

func hashString(s string) string {
	sum := md5.Sum([]byte(s)) //nolint:gosec
	return hex.EncodeToString(sum[:])
}

var mixinKeyEncTab = []int{
	46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
	27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
	37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
	22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52,
}

func signWBI(params map[string]string, imgKey, subKey string) url.Values {
	mixin := mixinKey(imgKey + subKey)
	vals := make(url.Values, len(params)+2)
	for k, v := range params {
		vals.Set(k, stripWBIChars(v))
	}
	wts := strconv.FormatInt(time.Now().Unix(), 10)
	vals.Set("wts", wts)

	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	pairs := make([]string, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, wbiEscape(k)+"="+wbiEscape(vals.Get(k)))
	}
	query := strings.Join(pairs, "&")
	wrid := hashString(query + mixin)
	vals.Set("w_rid", wrid)
	return vals
}

func mixinKey(raw string) string {
	if len(raw) < 64 {
		return raw
	}
	var b strings.Builder
	b.Grow(32)
	for _, idx := range mixinKeyEncTab {
		if idx >= 0 && idx < len(raw) {
			b.WriteByte(raw[idx])
		}
		if b.Len() >= 32 {
			break
		}
	}
	return b.String()
}

func stripWBIChars(v string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '!', '\'', '(', ')', '*':
			return -1
		default:
			return r
		}
	}, v)
}

func wbiEscape(v string) string {
	escaped := url.QueryEscape(v)
	return strings.ReplaceAll(escaped, "+", "%20")
}

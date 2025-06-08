package fetch

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"dash2hlsproxy/internal/mpd"
)

// SegmentDownloadTask 定义一个 SEGMENT 下载任务
type SegmentDownloadTask struct {
	ChannelID   string
	SegmentKey  string // 缓存键: /hls/{channelID}/{streamType}/{qualityOrLang}/{segmentName}
	UpstreamURL string
	UserAgent   string
}

// CachedSegment 包含下载的 SEGMENT 数据和元数据
type CachedSegment struct {
	Data        []byte
	ContentType string
	FetchedAt   time.Time
}

// Fetcher 负责获取上游数据
type Fetcher struct {
	HttpClient *http.Client
	Logger     *slog.Logger
}

// NewFetcher 创建一个新的 Fetcher
func NewFetcher(logger *slog.Logger, client *http.Client) *Fetcher {
	return &Fetcher{
		HttpClient: client,
		Logger:     logger,
	}
}

// FetchSegment 从指定的 URL 获取单个 SEGMENT
func (f *Fetcher) FetchSegment(ctx context.Context, url string, userAgent string) ([]byte, string, error) {
	var data []byte
	var contentType string
	var err error
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			// Exponential backoff: 1s, 2s
			time.Sleep(time.Second * time.Duration(1<<(i-1)))
		}

		var req *http.Request
		req, err = http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			err = fmt.Errorf("创建请求失败: %w", err)
			f.Logger.Error("Failed to create segment request", "url", url, "error", err)
			return nil, "", err
		}
		if userAgent != "" {
			req.Header.Set("User-Agent", userAgent)
		}

		var resp *http.Response
		resp, err = f.HttpClient.Do(req)
		if err != nil {
			err = fmt.Errorf("请求上游 SEGMENT 失败: %w", err)
			f.Logger.Warn("Failed to fetch segment, retrying...", "url", url, "retry", i+1, "error", err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("上游 SEGMENT 返回非 200 状态码: %s", resp.Status)
			f.Logger.Warn("Upstream segment fetch returned non-200 status, retrying...", "url", url, "status", resp.Status, "retry", i+1)
			continue
		}

		data, err = io.ReadAll(resp.Body)
		if err != nil {
			err = fmt.Errorf("读取 SEGMENT 数据失败: %w", err)
			f.Logger.Warn("Failed to read segment data, retrying...", "url", url, "retry", i+1, "error", err)
			continue
		}
		contentType = resp.Header.Get("Content-Type")
		return data, contentType, nil // Success
	}

	f.Logger.Error("Failed to fetch segment after all retries", "url", url, "error", err)
	return nil, "", err
}

// FetchMPDWithRetry 获取并解析 MPD，并在 baseURL 不匹配时重试
func (f *Fetcher) FetchMPDWithRetry(
	ctx context.Context,
	initialFetchURL, userAgent, knownInitialBaseURL string,
	isInitialBaseURLSet bool,
) (string, string, *mpd.MPD, error) {
	var finalURL string
	var mpdObj *mpd.MPD
	var err error
	const maxRetries = 3

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			// Exponential backoff: 1s, 2s
			time.Sleep(time.Second * time.Duration(1<<(i-1)))
		}
		finalURL, mpdObj, err = mpd.FetchAndParseMPD(ctx, f.HttpClient, initialFetchURL, userAgent)
		if err == nil {
			// Success
			break
		}
		f.Logger.Warn("Failed to fetch MPD, retrying...", "url", initialFetchURL, "retry", i+1, "error", err)
	}

	if err != nil {
		err := fmt.Errorf("initial fetch failed after %d retries: %w", maxRetries, err)
		f.Logger.Error("Initial MPD fetch failed", "url", initialFetchURL, "error", err)
		return initialFetchURL, "", nil, err
	}

	var actualBaseURL string
	if mpdObj != nil && len(mpdObj.BaseURLs) > 0 && mpdObj.BaseURLs[0] != "" {
		actualBaseURL = mpdObj.BaseURLs[0]
	}

	if !isInitialBaseURLSet || actualBaseURL == knownInitialBaseURL {
		return finalURL, actualBaseURL, mpdObj, nil
	}

	f.Logger.Warn("BaseURL mismatch. This scenario is not fully handled yet and might lead to issues.", "expected", knownInitialBaseURL, "got", actualBaseURL)
	// In a more robust implementation, we might retry with the new BaseURL.
	// For now, we just log and proceed.
	return finalURL, actualBaseURL, mpdObj, nil
}

// ParsePublishTime 解析 MPD 的 publishTime 字符串
func ParsePublishTime(pt string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, pt)
	if err != nil {
		// Fallback for a slightly different format
		t, err = time.Parse("2006-01-02T15:04:05Z", pt)
	}
	return t, err
}

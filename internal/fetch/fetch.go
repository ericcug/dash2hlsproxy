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
func (f *Fetcher) FetchSegment(url string, userAgent string) ([]byte, string, error) {
	var lastErr error
	maxRetries := 3
	retryDelay := 500 * time.Millisecond
	requestTimeout := 8 * time.Second

	for i := 0; i < maxRetries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		defer cancel()

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, "", fmt.Errorf("创建请求失败: %w", err)
		}
		if userAgent != "" {
			req.Header.Set("User-Agent", userAgent)
		}

		resp, err := f.HttpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("请求上游 SEGMENT 失败 (attempt %d/%d): %w", i+1, maxRetries, err)
			time.Sleep(retryDelay)
			continue
		}

		if resp.StatusCode == http.StatusOK {
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, "", fmt.Errorf("读取 SEGMENT 数据失败: %w", err)
			}
			contentType := resp.Header.Get("Content-Type")
			return data, contentType, nil
		}

		lastErr = fmt.Errorf("上游 SEGMENT 返回非 200 状态码 (attempt %d/%d): %s", i+1, maxRetries, resp.Status)
		resp.Body.Close()

		if resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout && resp.StatusCode != http.StatusGatewayTimeout {
			break
		}

		time.Sleep(retryDelay)
	}

	return nil, "", lastErr
}

// FetchMPDWithRetry 获取并解析 MPD，并在 baseURL 不匹配时重试
func (f *Fetcher) FetchMPDWithRetry(
	initialFetchURL, userAgent, knownInitialBaseURL string,
	isInitialBaseURLSet bool,
) (string, string, *mpd.MPD, error) {
	finalURL, mpdObj, err := mpd.FetchAndParseMPD(initialFetchURL, userAgent)
	if err != nil {
		return initialFetchURL, "", nil, fmt.Errorf("initial fetch failed: %w", err)
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

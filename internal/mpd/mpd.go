package mpd

import (
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// ContentProtection represents the ContentProtection XML element.
type ContentProtection struct {
	XMLName        xml.Name `xml:"ContentProtection"`
	SchemeIDURI    string   `xml:"schemeIdUri,attr"`
	Value          string   `xml:"value,attr,omitempty"`
	CencDefaultKID string   `xml:"urn:mpeg:cenc:2013 default_KID,attr,omitempty"` // Parses cenc:default_KID attribute
	PSSH           string   `xml:"pssh,omitempty"`
	MsprPro        string   `xml:"pro,omitempty"`
}

// S represents a segment in the SegmentTimeline.
type S struct {
	XMLName xml.Name `xml:"S"`
	T       *uint64  `xml:"t,attr"`
	D       uint64   `xml:"d,attr"`
	R       *int     `xml:"r,attr"`
}

// SegmentTimeline represents the SegmentTimeline XML element.
type SegmentTimeline struct {
	XMLName  xml.Name `xml:"SegmentTimeline"`
	Segments []S      `xml:"S"`
}

// SegmentTemplate represents the SegmentTemplate XML element.
type SegmentTemplate struct {
	XMLName                xml.Name         `xml:"SegmentTemplate"`
	Timescale              *uint64          `xml:"timescale,attr"`
	Initialization         string           `xml:"initialization,attr,omitempty"`
	Media                  string           `xml:"media,attr"`
	PresentationTimeOffset *uint64          `xml:"presentationTimeOffset,attr,omitempty"`
	SegmentTimeline        *SegmentTimeline `xml:"SegmentTimeline"`
}

// Representation represents the Representation XML element.
type Representation struct {
	XMLName            xml.Name `xml:"Representation"`
	ID                 string   `xml:"id,attr"`
	Bandwidth          uint     `xml:"bandwidth,attr"`
	Width              uint     `xml:"width,attr,omitempty"`
	Height             uint     `xml:"height,attr,omitempty"`
	FrameRate          string   `xml:"frameRate,attr,omitempty"`
	Sar                string   `xml:"sar,attr,omitempty"`
	Codecs             string   `xml:"codecs,attr,omitempty"`
	MimeType           string   `xml:"mimeType,attr,omitempty"`
	AudioSamplingRate  string   `xml:"audioSamplingRate,attr,omitempty"`
	AudioChannelConfig *struct {
		SchemeIDURI string `xml:"schemeIdUri,attr"`
		Value       string `xml:"value,attr"`
	} `xml:"AudioChannelConfiguration,omitempty"`
	SegmentTemplate *SegmentTemplate `xml:"SegmentTemplate"`
}

// AdaptationSet represents the AdaptationSet XML element.
type AdaptationSet struct {
	XMLName            xml.Name            `xml:"AdaptationSet"`
	ID                 string              `xml:"id,attr,omitempty"`
	ContentType        string              `xml:"contentType,attr,omitempty"`
	Lang               string              `xml:"lang,attr,omitempty"`
	MimeType           string              `xml:"mimeType,attr"`
	SegmentAlignment   bool                `xml:"segmentAlignment,attr,omitempty"`
	StartWithSAP       int                 `xml:"startWithSAP,attr,omitempty"`
	MaxWidth           uint                `xml:"maxWidth,attr,omitempty"`
	MaxHeight          uint                `xml:"maxHeight,attr,omitempty"`
	MinFrameRate       string              `xml:"minFrameRate,attr,omitempty"`
	MaxFrameRate       string              `xml:"maxFrameRate,attr,omitempty"`
	Par                string              `xml:"par,attr,omitempty"`
	ContentProtections []ContentProtection `xml:"ContentProtection"`
	Representations    []Representation    `xml:"Representation"`
	SegmentTemplate    *SegmentTemplate    `xml:"SegmentTemplate"`
	BaseURL            string              `xml:"BaseURL,omitempty"`
}

// Period represents the Period XML element.
type Period struct {
	XMLName        xml.Name        `xml:"Period"`
	ID             string          `xml:"id,attr,omitempty"`
	Start          string          `xml:"start,attr,omitempty"`
	BaseURLs       []string        `xml:"BaseURL"`
	AdaptationSets []AdaptationSet `xml:"AdaptationSet"`
}

// MPD represents the root MPD XML element.
type MPD struct {
	XMLName               xml.Name `xml:"urn:mpeg:dash:schema:mpd:2011 MPD"`
	Profiles              string   `xml:"profiles,attr"`
	Type                  string   `xml:"type,attr"`
	MinimumUpdatePeriod   string   `xml:"minimumUpdatePeriod,attr,omitempty"`
	TimeShiftBufferDepth  string   `xml:"timeShiftBufferDepth,attr,omitempty"`
	AvailabilityStartTime string   `xml:"availabilityStartTime,attr,omitempty"`
	PublishTime           string   `xml:"publishTime,attr,omitempty"`
	MaxSegmentDuration    string   `xml:"maxSegmentDuration,attr,omitempty"`
	MinBufferTime         string   `xml:"minBufferTime,attr,omitempty"`
	Periods               []Period `xml:"Period"`
	BaseURLs              []string `xml:"BaseURL"`
}

func parseDuration(dashDuration string) (time.Duration, error) {
	if !strings.HasPrefix(dashDuration, "PT") {
		return 0, fmt.Errorf("invalid DASH duration format: missing PT prefix: %s", dashDuration)
	}
	trimmed := strings.TrimPrefix(dashDuration, "PT")
	if strings.Contains(trimmed, "H") || strings.Contains(trimmed, "M") {
		if strings.HasSuffix(trimmed, "S") && !strings.Contains(trimmed, "H") && !strings.Contains(trimmed, "M") {
			return time.ParseDuration(strings.ToLower(trimmed))
		}
		return 0, fmt.Errorf("complex DASH duration parsing not yet fully implemented for: %s", dashDuration)
	}
	return time.ParseDuration(strings.ToLower(trimmed))
}

func (m *MPD) GetMinimumUpdatePeriod() (time.Duration, error) {
	if m.MinimumUpdatePeriod == "" {
		return 0, fmt.Errorf("MinimumUpdatePeriod is not set in MPD")
	}
	return parseDuration(m.MinimumUpdatePeriod)
}

func (m *MPD) GetMaxSegmentDuration() (time.Duration, error) {
	if m.MaxSegmentDuration == "" {
		return 0, fmt.Errorf("MaxSegmentDuration is not set in MPD and inference is not yet implemented")
	}
	return parseDuration(m.MaxSegmentDuration)
}

func FetchAndParseMPD(initialMPDURL string, userAgent string) (string, *MPD, error) {
	client := &http.Client{
		Timeout: 15 * time.Second,
	}
	req, err := http.NewRequest("GET", initialMPDURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request for MPD: %w", err)
	}
	if userAgent != "" {
		req.Header.Set("User-Agent", userAgent)
	}
	req.Header.Set("Accept", "application/dash+xml,application/xml,text/xml")

	resp, err := client.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("failed to fetch MPD from %s: %w", initialMPDURL, err)
	}
	defer resp.Body.Close()

	finalMPDURL := resp.Request.URL.String()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return finalMPDURL, nil, fmt.Errorf("failed to fetch MPD: status %s, initial_url %s, final_url %s, body: %s", resp.Status, initialMPDURL, finalMPDURL, string(bodyBytes))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return finalMPDURL, nil, fmt.Errorf("failed to read MPD response body: %w", err)
	}

	var mpdData MPD
	if err := xml.Unmarshal(body, &mpdData); err != nil {
		bodySnippet := string(body)
		if len(bodySnippet) > 512 {
			bodySnippet = bodySnippet[:512] + "..."
		}
		return finalMPDURL, nil, fmt.Errorf("failed to unmarshal MPD XML: %w. XML Body Snippet: %s", err, bodySnippet)
	}
	return finalMPDURL, &mpdData, nil
}

func (as *AdaptationSet) GetDefaultKID() string {
	if as == nil {
		log.Println("GetDefaultKID called on nil AdaptationSet")
		return ""
	}
	log.Printf("GetDefaultKID: Checking AdaptationSet ID '%s', ContentType '%s', Lang '%s'. Found %d ContentProtection elements.", as.ID, as.ContentType, as.Lang, len(as.ContentProtections))
	for i, cp := range as.ContentProtections {
		log.Printf("GetDefaultKID: ContentProtection[%d]: SchemeIDURI='%s', Value='%s', CencDefaultKID_Attr='%s'", i, cp.SchemeIDURI, cp.Value, cp.CencDefaultKID)
		isCENCSystem := strings.ToLower(cp.SchemeIDURI) == "urn:mpeg:dash:mp4protection:2011" && cp.Value == "cenc"

		if isCENCSystem {
			if cp.CencDefaultKID != "" {
				kid := strings.ReplaceAll(cp.CencDefaultKID, "-", "")
				log.Printf("GetDefaultKID: Found CENC ContentProtection. KID from MPD attribute: '%s', Processed KID: '%s'", cp.CencDefaultKID, kid)
				return kid
			} else {
				log.Printf("GetDefaultKID: CENC ContentProtection found, but cenc:default_KID attribute is empty or missing.")
			}
		}
	}
	log.Printf("GetDefaultKID: No suitable cenc:default_KID found in AdaptationSet ID '%s'", as.ID)
	return ""
}

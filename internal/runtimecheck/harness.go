package runtimecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

type harness struct {
	root       string
	statePath  string
	repo       *storage.FileRepository
	ids        *idgen.Counter
	timeSource *clock.Fixed
	server     *httptest.Server
	client     *http.Client
}

func newHarness() (*harness, error) {
	root, err := os.MkdirTemp("", "segmentd-workflow-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary root: %w", err)
	}
	statePath := filepath.Join(root, "state.json")
	repo, err := storage.NewFileRepository(statePath)
	if err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	value := &harness{
		root:       root,
		statePath:  statePath,
		repo:       repo,
		ids:        &idgen.Counter{},
		timeSource: clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	if err := value.restart(); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	return value, nil
}

func (h *harness) restart() error {
	if h.server != nil {
		h.server.Close()
		h.server = nil
	}
	service, err := application.NewService(
		h.repo,
		h.ids,
		h.timeSource,
		observability.Discard{},
		application.Limits{
			MaxAppendRecords: 100,
			MaxAppendBytes:   1 << 20,
		},
	)
	if err != nil {
		return fmt.Errorf("construct service: %w", err)
	}
	api := httpapi.New(service, httpapi.Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  3 * time.Second,
		Logger:          observability.Discard{},
	})
	h.server = httptest.NewServer(api.Handler())
	return nil
}

func (h *harness) close() {
	if h.server != nil {
		h.server.Close()
	}
	_ = os.RemoveAll(h.root)
}

func (h *harness) call(
	ctx context.Context,
	method string,
	path string,
	body any,
	target any,
) error {
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, h.server.URL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()

	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf(
			"request %s %s returned %d: %s",
			method,
			path,
			response.StatusCode,
			string(data),
		)
	}
	if target != nil {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(target); err != nil {
			return fmt.Errorf("decode response for %s %s: %w", method, path, err)
		}
	}
	return nil
}

func (h *harness) callExpect(
	ctx context.Context,
	method string,
	path string,
	body any,
	expectedStatus int,
	target any,
) error {
	var reader io.Reader = http.NoBody
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request body: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, h.server.URL+path, reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if response.StatusCode != expectedStatus {
		return fmt.Errorf(
			"request %s %s returned %d, want %d: %s",
			method,
			path,
			response.StatusCode,
			expectedStatus,
			string(data),
		)
	}
	if target != nil {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(target); err != nil {
			return fmt.Errorf("decode response for %s %s: %w", method, path, err)
		}
	}
	return nil
}

type rawResponse struct {
	Status int
	Body   []byte
}

// callRaw sends a verbatim request body (including "null") so malformed
// shapes produced by serialization tooling can be exercised directly.
func (h *harness) callRaw(
	ctx context.Context,
	method string,
	path string,
	body string,
) (rawResponse, error) {
	var reader io.Reader = http.NoBody
	if body != "" {
		reader = strings.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, h.server.URL+path, reader)
	if err != nil {
		return rawResponse{}, fmt.Errorf("build request: %w", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return rawResponse{}, fmt.Errorf("perform request: %w", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return rawResponse{}, fmt.Errorf("read response: %w", err)
	}
	return rawResponse{Status: response.StatusCode, Body: data}, nil
}

func (h *harness) snapshotBytes() ([]byte, error) {
	return os.ReadFile(h.statePath)
}

func (h *harness) loadSnapshot() (*domain.State, error) {
	data, err := os.ReadFile(h.statePath)
	if err != nil {
		return nil, err
	}
	var state domain.State
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&state); err != nil {
		return nil, err
	}
	return &state, nil
}

type segmentList struct {
	Items []*domain.Segment `json:"items"`
}

func listSegments(
	ctx context.Context,
	h *harness,
	streamID string,
) ([]segmentView, error) {
	var response segmentList
	if err := h.call(
		ctx,
		http.MethodGet,
		"/v1/streams/"+streamID+"/segments?limit=100",
		nil,
		&response,
	); err != nil {
		return nil, err
	}
	result := make([]segmentView, 0, len(response.Items))
	for _, item := range response.Items {
		if item == nil {
			continue
		}
		result = append(result, segmentView{
			ID:          item.ID,
			StreamID:    item.StreamID,
			Generation:  item.Generation,
			State:       string(item.State),
			BaseOffset:  item.Range.BaseOffset,
			EndOffset:   item.Range.EndOffset,
			RecordCount: item.RecordCount,
			SizeBytes:   item.SizeBytes,
		})
	}
	return result, nil
}

type segmentView struct {
	ID          string
	StreamID    string
	Generation  uint64
	State       string
	BaseOffset  int64
	EndOffset   int64
	RecordCount uint64
	SizeBytes   int64
}

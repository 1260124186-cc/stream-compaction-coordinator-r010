package httpapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/clock"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/httpapi"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/idgen"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/observability"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/storage"
)

// testServer wires the real HTTP API, application service, and file
// repository together exactly like the production entry point, so rejected
// requests can be checked against the durable snapshot on disk.
type testServer struct {
	t         *testing.T
	dir       string
	statePath string
	repo      *storage.FileRepository
	server    *httptest.Server
	client    *http.Client
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()
	dir := t.TempDir()
	repo, err := storage.NewFileRepository(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	ts := &testServer{
		t:         t,
		dir:       dir,
		statePath: repo.Path(),
		repo:      repo,
		client:    &http.Client{Timeout: 5 * time.Second},
	}
	ts.restart()
	t.Cleanup(func() { ts.server.Close() })
	return ts
}

// restart rebuilds the service from the same repository, simulating a
// process restart over the durable snapshot.
func (ts *testServer) restart() {
	if ts.server != nil {
		ts.server.Close()
	}
	service, err := application.NewService(
		ts.repo,
		&idgen.Counter{},
		clock.NewFixed(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), time.Second),
		observability.Discard{},
		application.Limits{MaxAppendRecords: 100, MaxAppendBytes: 1 << 20},
	)
	if err != nil {
		ts.t.Fatalf("construct service: %v", err)
	}
	api := httpapi.New(service, httpapi.Options{
		MaxRequestBytes: 1 << 20,
		RequestTimeout:  3 * time.Second,
		Logger:          observability.Discard{},
	})
	ts.server = httptest.NewServer(api.Handler())
}

type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (ts *testServer) post(t *testing.T, path string, body string) (int, []byte) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, ts.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := ts.client.Do(request)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return response.StatusCode, data
}

func (ts *testServer) get(t *testing.T, path string) []byte {
	t.Helper()
	response, err := ts.client.Get(ts.server.URL + path)
	if err != nil {
		t.Fatalf("perform request: %v", err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", path, response.StatusCode, string(data))
	}
	return data
}

func (ts *testServer) stateFileBytes(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(ts.statePath)
	if err != nil {
		t.Fatalf("read state file: %v", err)
	}
	return data
}

func (ts *testServer) persistedState(t *testing.T) *domain.State {
	t.Helper()
	state, err := storage.UnmarshalState(ts.stateFileBytes(t))
	if err != nil {
		t.Fatalf("decode state file: %v", err)
	}
	return state
}

// requireOnlyStateFile asserts the storage directory holds exactly the
// snapshot and no leftover temporary siblings from abandoned writes.
func (ts *testServer) requireOnlyStateFile(t *testing.T) {
	t.Helper()
	entries, err := os.ReadDir(ts.dir)
	if err != nil {
		t.Fatalf("list state directory: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "state.json" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Fatalf("state directory contains unexpected files: %v", names)
	}
}

func (ts *testServer) requireNullRejected(t *testing.T, path string) {
	t.Helper()
	status, body := ts.post(t, path, "null")
	if status != http.StatusBadRequest {
		t.Fatalf("POST %s with null returned %d, want 400: %s", path, status, string(body))
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if envelope.Error.Code != string(domain.CodeInvalidInput) {
		t.Fatalf("POST %s with null returned code %q, want %q",
			path, envelope.Error.Code, domain.CodeInvalidInput)
	}
	if envelope.Error.Message != "request body must be a JSON object" {
		t.Fatalf("POST %s with null returned message %q", path, envelope.Error.Message)
	}
}

func (ts *testServer) createStream(t *testing.T, name string, threshold int64) string {
	t.Helper()
	status, body := ts.post(t, "/v1/streams", `{
		"namespace": "field-lab",
		"name": "`+name+`",
		"threshold_bytes": `+jsonNumber(threshold)+`
	}`)
	if status != http.StatusCreated {
		t.Fatalf("create stream returned %d: %s", status, string(body))
	}
	var result application.CreateStreamResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode create stream response: %v", err)
	}
	return result.Stream.ID
}

func (ts *testServer) appendRecords(t *testing.T, streamID string, revision uint64, payloadBytes int64) {
	t.Helper()
	status, body := ts.post(t, "/v1/streams/"+streamID+"/records", `{
		"expected_revision": `+jsonNumber(int64(revision))+`,
		"record_count": 2,
		"payload_bytes": `+jsonNumber(payloadBytes)+`
	}`)
	if status != http.StatusOK {
		t.Fatalf("append records returned %d: %s", status, string(body))
	}
}

func (ts *testServer) sealedSegmentIDs(t *testing.T, streamID string) []string {
	t.Helper()
	var list struct {
		Items []*domain.Segment `json:"items"`
	}
	if err := json.Unmarshal(ts.get(t, "/v1/streams/"+streamID+"/segments?limit=100"), &list); err != nil {
		t.Fatalf("decode segment list: %v", err)
	}
	ids := make([]string, 0)
	for _, segment := range list.Items {
		if segment != nil && segment.State == domain.SegmentStateSealed {
			ids = append(ids, segment.ID)
		}
	}
	return ids
}

func (ts *testServer) createPlan(t *testing.T, streamID string, sources []string, revision uint64) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{
		"source_segment_ids":       sources,
		"expected_stream_revision": revision,
	})
	if err != nil {
		t.Fatalf("encode plan request: %v", err)
	}
	status, body := ts.post(t, "/v1/streams/"+streamID+"/compaction-plans", string(encoded))
	if status != http.StatusCreated {
		t.Fatalf("create plan returned %d: %s", status, string(body))
	}
	var result application.CompactionPlanResult
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode create plan response: %v", err)
	}
	return result.Plan.ID
}

func (ts *testServer) getPlan(t *testing.T, planID string) domain.CompactionPlan {
	t.Helper()
	var plan domain.CompactionPlan
	if err := json.Unmarshal(ts.get(t, "/v1/compaction-plans/"+planID), &plan); err != nil {
		t.Fatalf("decode plan: %v", err)
	}
	return plan
}

func (ts *testServer) getSegment(t *testing.T, segmentID string) domain.Segment {
	t.Helper()
	var segment domain.Segment
	if err := json.Unmarshal(ts.get(t, "/v1/segments/"+segmentID), &segment); err != nil {
		t.Fatalf("decode segment: %v", err)
	}
	return segment
}

// preparePlan builds one stream with two sealed source segments and one
// proposed compaction plan over them.
func (ts *testServer) preparePlan(t *testing.T) (string, []string, string) {
	t.Helper()
	streamID := ts.createStream(t, "null-body-flow", 32)
	for revision := uint64(1); revision <= 4; revision++ {
		ts.appendRecords(t, streamID, revision, 20)
	}
	sources := ts.sealedSegmentIDs(t, streamID)
	if len(sources) != 2 {
		t.Fatalf("expected two sealed source segments, got %v", sources)
	}
	planID := ts.createPlan(t, streamID, sources, 5)
	return streamID, sources, planID
}

func jsonNumber(value int64) string {
	return strings.TrimSpace(jsonMust(json.Marshal(value)))
}

func jsonMust(data []byte, err error) string {
	if err != nil {
		panic(err)
	}
	return string(data)
}

// TestNullBodyRejectedWithoutStateChange sends a top-level JSON null to every
// write endpoint and proves the rejection leaves no trace: the durable
// snapshot, the audit log, and every readable detail stay identical, even
// across a restart.
func TestNullBodyRejectedWithoutStateChange(t *testing.T) {
	ts := newTestServer(t)
	streamID, sources, planID := ts.preparePlan(t)

	baselineFile := ts.stateFileBytes(t)
	baselineState := ts.persistedState(t)
	baselinePlan := ts.get(t, "/v1/compaction-plans/"+planID)
	baselineStream := ts.get(t, "/v1/streams/"+streamID)
	baselineSegments := ts.get(t, "/v1/streams/"+streamID+"/segments?limit=100")

	endpoints := []struct {
		name string
		path string
	}{
		{"create stream", "/v1/streams"},
		{"append records", "/v1/streams/" + streamID + "/records"},
		{"create compaction plan", "/v1/streams/" + streamID + "/compaction-plans"},
		{"start compaction plan", "/v1/compaction-plans/" + planID + "/start"},
		{"finish compaction plan", "/v1/compaction-plans/" + planID + "/finish"},
		{"acknowledge replica", "/v1/segments/" + sources[0] + "/replica-acks"},
		{"admin recover", "/v1/admin/recover"},
	}
	for _, endpoint := range endpoints {
		t.Run(endpoint.name, func(t *testing.T) {
			ts.requireNullRejected(t, endpoint.path)
			if current := ts.stateFileBytes(t); !bytes.Equal(current, baselineFile) {
				t.Fatalf("state file changed after rejected request to %s", endpoint.path)
			}
			ts.requireOnlyStateFile(t)
		})
	}

	if current := ts.get(t, "/v1/compaction-plans/"+planID); !bytes.Equal(current, baselinePlan) {
		t.Fatalf("plan details changed after rejected requests: %s", string(current))
	}
	if current := ts.get(t, "/v1/streams/"+streamID); !bytes.Equal(current, baselineStream) {
		t.Fatalf("stream details changed after rejected requests: %s", string(current))
	}
	if current := ts.get(t, "/v1/streams/"+streamID+"/segments?limit=100"); !bytes.Equal(current, baselineSegments) {
		t.Fatalf("segment details changed after rejected requests: %s", string(current))
	}
	persisted := ts.persistedState(t)
	if persisted.AuditSequence != baselineState.AuditSequence ||
		len(persisted.Audit) != len(baselineState.Audit) {
		t.Fatalf("audit log changed after rejected requests: sequence %d -> %d",
			baselineState.AuditSequence, persisted.AuditSequence)
	}
	if plan := persisted.Plans[planID]; plan == nil || plan.State != domain.PlanStateProposed {
		t.Fatalf("persisted plan is not proposed after rejected requests: %+v", plan)
	}

	// A restart over the snapshot must find nothing to recover and must not
	// rewrite the file; the rejected requests left no work behind.
	ts.restart()
	if current := ts.stateFileBytes(t); !bytes.Equal(current, baselineFile) {
		t.Fatalf("restart rewrote the state file after rejected requests")
	}
	if current := ts.get(t, "/v1/compaction-plans/"+planID); !bytes.Equal(current, baselinePlan) {
		t.Fatalf("plan details differ after restart: %s", string(current))
	}
	if current := ts.get(t, "/v1/streams/"+streamID); !bytes.Equal(current, baselineStream) {
		t.Fatalf("stream details differ after restart: %s", string(current))
	}
	if current := ts.get(t, "/v1/streams/"+streamID+"/segments?limit=100"); !bytes.Equal(current, baselineSegments) {
		t.Fatalf("segment details differ after restart: %s", string(current))
	}
}

// TestNullBodyDoesNotAdvancePlanOrRecovery focuses on the two endpoints that
// accept an empty object: a null body must not start a plan and must not run
// recovery, while an explicit empty object keeps the current behavior.
func TestNullBodyDoesNotAdvancePlanOrRecovery(t *testing.T) {
	ts := newTestServer(t)
	_, sources, planID := ts.preparePlan(t)

	// A null body must not start the plan.
	before := ts.stateFileBytes(t)
	ts.requireNullRejected(t, "/v1/compaction-plans/"+planID+"/start")
	if !bytes.Equal(ts.stateFileBytes(t), before) {
		t.Fatalf("state file changed after null plan start")
	}
	if plan := ts.getPlan(t, planID); plan.State != domain.PlanStateProposed {
		t.Fatalf("plan state is %s after null start, want proposed", plan.State)
	}
	for _, sourceID := range sources {
		if segment := ts.getSegment(t, sourceID); segment.State != domain.SegmentStateSealed {
			t.Fatalf("source %s state is %s after null start, want sealed", sourceID, segment.State)
		}
	}

	// An explicit empty object still starts the plan.
	status, body := ts.post(t, "/v1/compaction-plans/"+planID+"/start", "{}")
	if status != http.StatusOK {
		t.Fatalf("empty-object plan start returned %d: %s", status, string(body))
	}
	if plan := ts.getPlan(t, planID); plan.State != domain.PlanStateRunning {
		t.Fatalf("plan state is %s after empty-object start, want running", plan.State)
	}
	for _, sourceID := range sources {
		if segment := ts.getSegment(t, sourceID); segment.State != domain.SegmentStateCompacting {
			t.Fatalf("source %s state is %s after start, want compacting", sourceID, segment.State)
		}
	}
	started := ts.stateFileBytes(t)

	// A null body must not trigger recovery while the plan is running.
	ts.requireNullRejected(t, "/v1/admin/recover")
	if !bytes.Equal(ts.stateFileBytes(t), started) {
		t.Fatalf("state file changed after null recovery")
	}
	if plan := ts.getPlan(t, planID); plan.State != domain.PlanStateRunning {
		t.Fatalf("plan state is %s after null recovery, want running", plan.State)
	}
	for _, event := range ts.persistedState(t).Audit {
		if event.Action == "compaction.plan.aborted_on_recovery" {
			t.Fatalf("null recovery wrote an abort audit event: %+v", event)
		}
	}

	// An explicit empty object still runs recovery and aborts the plan.
	status, body = ts.post(t, "/v1/admin/recover", "{}")
	if status != http.StatusOK {
		t.Fatalf("empty-object recovery returned %d: %s", status, string(body))
	}
	var report application.RecoveryReport
	if err := json.Unmarshal(body, &report); err != nil {
		t.Fatalf("decode recovery report: %v", err)
	}
	if !report.Changed || len(report.AbortedPlanIDs) != 1 || report.AbortedPlanIDs[0] != planID {
		t.Fatalf("recovery report does not abort the running plan: %+v", report)
	}
	if plan := ts.getPlan(t, planID); plan.State != domain.PlanStateAborted {
		t.Fatalf("plan state is %s after recovery, want aborted", plan.State)
	}
	for _, sourceID := range sources {
		if segment := ts.getSegment(t, sourceID); segment.State != domain.SegmentStateSealed {
			t.Fatalf("source %s state is %s after recovery, want sealed", sourceID, segment.State)
		}
	}

	// The persisted snapshot after a restart reflects exactly the accepted
	// mutations and nothing from the rejected null requests.
	recovered := ts.stateFileBytes(t)
	ts.restart()
	if !bytes.Equal(ts.stateFileBytes(t), recovered) {
		t.Fatalf("restart rewrote the state file after recovery")
	}
	if plan := ts.getPlan(t, planID); plan.State != domain.PlanStateAborted {
		t.Fatalf("plan state is %s after restart, want aborted", plan.State)
	}
}

// TestObjectShapeAndFieldValidationUnchanged locks the remaining decode
// contract: explicit empty objects still reach business validation, and the
// existing field-level errors are preserved.
func TestObjectShapeAndFieldValidationUnchanged(t *testing.T) {
	ts := newTestServer(t)

	// An explicit empty object passes the shape check and fails inside
	// business validation, not at the transport layer.
	status, body := ts.post(t, "/v1/streams", "{}")
	if status != http.StatusBadRequest {
		t.Fatalf("empty-object stream create returned %d: %s", status, string(body))
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if envelope.Error.Code != string(domain.CodeInvalidInput) ||
		envelope.Error.Message == "request body must be a JSON object" {
		t.Fatalf("empty object was rejected by shape validation instead of business rules: %s", string(body))
	}

	// Non-object top-level values are all rejected with invalid_input.
	for _, raw := range []string{"null", " null ", "[1,2]", `"text"`, "42", "true"} {
		status, body := ts.post(t, "/v1/streams", raw)
		if status != http.StatusBadRequest {
			t.Fatalf("body %s returned %d: %s", raw, status, string(body))
		}
		var rejected errorEnvelope
		if err := json.Unmarshal(body, &rejected); err != nil {
			t.Fatalf("decode error response for %s: %v", raw, err)
		}
		if rejected.Error.Code != string(domain.CodeInvalidInput) {
			t.Fatalf("body %s returned code %q, want %q", raw, rejected.Error.Code, domain.CodeInvalidInput)
		}
	}

	// Existing field-level validation keeps its behavior.
	cases := []struct {
		name    string
		body    string
		message string
	}{
		{"unknown field", `{"namespace":"a","name":"b","threshold_bytes":8,"extra":1}`, "unknown field"},
		{"wrong field type", `{"namespace":"a","name":"b","threshold_bytes":"x"}`, "wrong JSON type"},
		{"trailing content", `{} {}`, "trailing JSON content"},
		{"invalid syntax", `{broken`, "not valid JSON"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			status, body := ts.post(t, "/v1/streams", testCase.body)
			if status != http.StatusBadRequest {
				t.Fatalf("returned %d: %s", status, string(body))
			}
			var rejected errorEnvelope
			if err := json.Unmarshal(body, &rejected); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if rejected.Error.Code != string(domain.CodeInvalidInput) ||
				!strings.Contains(rejected.Error.Message, testCase.message) {
				t.Fatalf("unexpected error for %s: %s", testCase.name, string(body))
			}
		})
	}

	// A valid object is still accepted.
	status, body = ts.post(t, "/v1/streams", `{"namespace":"a","name":"b","threshold_bytes":8}`)
	if status != http.StatusCreated {
		t.Fatalf("valid stream create returned %d: %s", status, string(body))
	}
}

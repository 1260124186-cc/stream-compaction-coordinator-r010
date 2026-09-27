package runtimecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/application"
	"github.com/1260124186-cc/solo-0017-stream-compaction/internal/domain"
)

type apiErrorBody struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

// runNullObjectRejection verifies that every action endpoint requiring a JSON
// object rejects a top-level null (as produced by automation serializing an
// empty object) with invalid_input before any plan, audit, or snapshot change.
func runNullObjectRejection(
	ctx context.Context,
	assertions *counterAssertions,
) error {
	h, err := newHarness()
	if err != nil {
		return err
	}
	defer h.close()

	streamID, sourceIDs, err := prepareTwoSealedSegments(ctx, h, "null-guard-flow")
	if err != nil {
		return err
	}

	var planResult application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/compaction-plans",
		map[string]any{
			"source_segment_ids":       sourceIDs,
			"expected_stream_revision": 5,
		},
		&planResult,
	); err != nil {
		return err
	}
	planID := planResult.Plan.ID
	if err := assertions.require(
		planResult.Plan.State == domain.PlanStateProposed,
		"created plan is not proposed: %+v",
		planResult.Plan,
	); err != nil {
		return err
	}

	baseline, err := h.snapshotBytes()
	if err != nil {
		return err
	}
	baselineState, err := h.loadSnapshot()
	if err != nil {
		return err
	}

	// Start plan with null: must be rejected while the plan stays proposed.
	if err := requireNullRejected(ctx, h, assertions,
		"/v1/compaction-plans/"+planID+"/start", baseline,
	); err != nil {
		return err
	}
	if err := requirePlanState(ctx, h, assertions, planID, domain.PlanStateProposed); err != nil {
		return err
	}

	// Active recovery with null while a proposed plan exists: recovery must
	// not run at all (plan untouched, snapshot untouched).
	if err := requireNullRejected(ctx, h, assertions, "/v1/admin/recover", baseline); err != nil {
		return err
	}
	if err := requirePlanState(ctx, h, assertions, planID, domain.PlanStateProposed); err != nil {
		return err
	}

	// Other object-shaped write entries with null. None of them may execute
	// business validation or persist anything.
	otherNullEndpoints := []string{
		"/v1/streams",
		"/v1/streams/" + streamID + "/records",
		"/v1/streams/" + streamID + "/compaction-plans",
		"/v1/compaction-plans/" + planID + "/finish",
		"/v1/segments/" + sourceIDs[0] + "/replica-acks",
	}
	for _, endpoint := range otherNullEndpoints {
		if err := requireNullRejected(ctx, h, assertions, endpoint, baseline); err != nil {
			return err
		}
	}
	if err := requirePlanState(ctx, h, assertions, planID, domain.PlanStateProposed); err != nil {
		return err
	}

	// Non-object JSON scalars and arrays must be rejected the same way.
	for _, body := range []string{`0`, `true`, `""`, `[]`} {
		if err := requireNonObjectRejected(ctx, h, assertions, "/v1/streams", body, baseline); err != nil {
			return err
		}
	}

	// Explicit empty object on the empty-payload actions keeps working and
	// performs the real state operation (start advances the plan).
	var started application.CompactionPlanResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/compaction-plans/"+planID+"/start",
		map[string]any{},
		&started,
	); err != nil {
		return err
	}
	if err := assertions.require(
		started.Plan.State == domain.PlanStateRunning,
		"empty-object start did not run the plan: %+v",
		started.Plan,
	); err != nil {
		return err
	}

	runningBaseline, err := h.snapshotBytes()
	if err != nil {
		return err
	}
	runningState, err := h.loadSnapshot()
	if err != nil {
		return err
	}

	// While the plan is running, active recovery would abort it. A null
	// request must be rejected without aborting the plan or writing audit.
	if err := requireNullRejected(ctx, h, assertions, "/v1/admin/recover", runningBaseline); err != nil {
		return err
	}
	if err := requirePlanState(ctx, h, assertions, planID, domain.PlanStateRunning); err != nil {
		return err
	}

	// Null finish on the running plan must not complete/retire anything.
	if err := requireNullRejected(ctx, h, assertions,
		"/v1/compaction-plans/"+planID+"/finish", runningBaseline,
	); err != nil {
		return err
	}
	if err := requirePlanState(ctx, h, assertions, planID, domain.PlanStateRunning); err != nil {
		return err
	}

	// Ordinary field validation rules are unchanged: empty objects on
	// business endpoints still reach the domain validation and fail there;
	// unknown fields remain rejected by the decoder.
	fieldChecks := []struct {
		path string
		body string
	}{
		{"/v1/streams", `{}`},
		{"/v1/streams/" + streamID + "/records", `{}`},
		{"/v1/streams/" + streamID + "/compaction-plans", `{}`},
		{"/v1/compaction-plans/" + planID + "/finish", `{}`},
		{"/v1/segments/" + sourceIDs[0] + "/replica-acks", `{}`},
		{"/v1/streams", `{"namespace":"x","name":"y","threshold_bytes":16,"bogus":1}`},
	}
	for _, check := range fieldChecks {
		if err := requireStatus(ctx, h, assertions,
			http.MethodPost, check.path, check.body, http.StatusBadRequest,
		); err != nil {
			return err
		}
	}

	// A valid business request still succeeds after the null rejections.
	var appended application.AppendRecordsResult
	if err := h.call(
		ctx,
		http.MethodPost,
		"/v1/streams/"+streamID+"/records",
		map[string]any{
			"expected_revision": 5,
			"record_count":      1,
			"payload_bytes":     1,
		},
		&appended,
	); err != nil {
		return err
	}

	// Restart: the running plan is recovered through the standard startup
	// path. The durable snapshot must contain no audit events attributable
	// to the rejected null requests: only the legitimate start and append
	// events were recorded between baseline and the restart.
	if err := h.restart(); err != nil {
		return err
	}
	persisted, err := h.loadSnapshot()
	if err != nil {
		return err
	}
	wantSequence := runningState.AuditSequence + 2 // valid append, then startup abort
	if err := assertions.require(
		persisted.AuditSequence == wantSequence,
		"audit sequence after restart is %d, want %d (rejected nulls must leave no audit events)",
		persisted.AuditSequence, wantSequence,
	); err != nil {
		return err
	}
	if err := assertions.require(
		persisted.AuditSequence-baselineState.AuditSequence == 3,
		"expected exactly start + append + startup-abort audit events, got delta %d",
		persisted.AuditSequence-baselineState.AuditSequence,
	); err != nil {
		return err
	}

	var aborted domain.CompactionPlan
	if err := h.call(
		ctx, http.MethodGet,
		"/v1/compaction-plans/"+planID, nil, &aborted,
	); err != nil {
		return err
	}
	if err := assertions.require(
		aborted.State == domain.PlanStateAborted,
		"plan after restart is %s, want aborted",
		aborted.State,
	); err != nil {
		return err
	}
	for _, segmentID := range sourceIDs {
		var segment domain.Segment
		if err := h.call(
			ctx, http.MethodGet,
			"/v1/segments/"+segmentID, nil, &segment,
		); err != nil {
			return err
		}
		if err := assertions.require(
			segment.State == domain.SegmentStateSealed,
			"recovered source %s is %s, want sealed",
			segment.ID, segment.State,
		); err != nil {
			return err
		}
	}

	// Explicit empty object on active recovery still executes: it is a
	// no-op now that the running plan was aborted at startup.
	if err := h.callExpect(
		ctx, http.MethodPost, "/v1/admin/recover",
		map[string]any{}, http.StatusOK, nil,
	); err != nil {
		return err
	}
	return nil
}

func requireNullRejected(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	path string,
	expectedSnapshot []byte,
) error {
	return requireRejectedShape(ctx, h, assertions, path, "null", "null", expectedSnapshot)
}

func requireNonObjectRejected(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	path string,
	body string,
	expectedSnapshot []byte,
) error {
	received := nonObjectDescription(body)
	return requireRejectedShape(ctx, h, assertions, path, body, received, expectedSnapshot)
}

func requireRejectedShape(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	path string,
	body string,
	receivedDetail string,
	expectedSnapshot []byte,
) error {
	response, err := h.callRaw(ctx, http.MethodPost, path, body)
	if err != nil {
		return err
	}
	if err := assertions.require(
		response.Status == http.StatusBadRequest,
		"%s: %s returned %d, want %d: %s",
		body, path, response.Status, http.StatusBadRequest, string(response.Body),
	); err != nil {
		return err
	}
	var envelope apiErrorBody
	if err := json.Unmarshal(response.Body, &envelope); err != nil {
		return fmt.Errorf("%s: decode error response: %w", path, err)
	}
	if err := assertions.require(
		envelope.Error.Code == string(domain.CodeInvalidInput),
		"%s: %s returned code %q, want invalid_input",
		body, path, envelope.Error.Code,
	); err != nil {
		return err
	}
	if err := assertions.require(
		envelope.Error.Details["received"] == receivedDetail,
		"%s: %s detail received=%v, want %q",
		body, path, envelope.Error.Details["received"], receivedDetail,
	); err != nil {
		return err
	}
	actual, err := os.ReadFile(h.statePath)
	if err != nil {
		return err
	}
	if err := assertions.require(
		bytes.Equal(actual, expectedSnapshot),
		"%s: %s changed the snapshot file",
		body, path,
	); err != nil {
		return err
	}
	return nil
}

func requirePlanState(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	planID string,
	want domain.PlanState,
) error {
	var plan domain.CompactionPlan
	if err := h.call(
		ctx, http.MethodGet,
		"/v1/compaction-plans/"+planID, nil, &plan,
	); err != nil {
		return err
	}
	return assertions.require(
		plan.State == want,
		"plan %s state is %s, want %s",
		planID, plan.State, want,
	)
}

func requireStatus(
	ctx context.Context,
	h *harness,
	assertions *counterAssertions,
	method string,
	path string,
	body string,
	expectedStatus int,
) error {
	response, err := h.callRaw(ctx, method, path, body)
	if err != nil {
		return err
	}
	return assertions.require(
		response.Status == expectedStatus,
		"%s %s returned %d, want %d: %s",
		method, path, response.Status, expectedStatus, string(response.Body),
	)
}

func nonObjectDescription(body string) string {
	switch body {
	case `0`:
		return "number"
	case `true`:
		return "boolean"
	case `""`:
		return "string"
	case `[]`:
		return "array"
	default:
		return "json"
	}
}

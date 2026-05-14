package auth

import (
	"context"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type affinityExecutor struct {
	mu    sync.Mutex
	calls []string
}

func (e *affinityExecutor) Identifier() string { return "openai" }

func (e *affinityExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	_ = ctx
	_ = req
	_ = opts
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	e.mu.Lock()
	e.calls = append(e.calls, authID)
	e.mu.Unlock()
	return cliproxyexecutor.Response{Payload: []byte(authID)}, nil
}

func (e *affinityExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, &Error{Code: "not_implemented", Message: "ExecuteStream not implemented"}
}

func (e *affinityExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }

func (e *affinityExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{Payload: []byte("0")}, nil
}

func (e *affinityExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, &Error{Code: "not_implemented", Message: "HttpRequest not implemented", HTTPStatus: http.StatusNotImplemented}
}

func (e *affinityExecutor) Calls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.calls))
	copy(out, e.calls)
	return out
}

func registerAffinityTestAuths(t *testing.T, manager *Manager, model string, auths ...*Auth) {
	t.Helper()

	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		reg.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model, Created: time.Now().Unix()}})
		t.Cleanup(func(id string) func() {
			return func() { reg.UnregisterClient(id) }
		}(auth.ID))
		if _, err := manager.Register(context.Background(), auth); err != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, err)
		}
	}
}

func TestManagerSessionAffinityPrefersPreviousSuccessfulAuth(t *testing.T) {
	t.Parallel()

	const model = "gpt-4.1"
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &affinityExecutor{}
	manager.RegisterExecutor(executor)

	registerAffinityTestAuths(t, manager, model,
		&Auth{ID: "auth-a", Provider: "openai", Status: StatusActive},
		&Auth{ID: "auth-b", Provider: "openai", Status: StatusActive},
	)

	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SessionAffinityMetadataKey: "session-1",
		},
	}

	first, err := manager.Execute(context.Background(), []string{"openai"}, cliproxyexecutor.Request{Model: model}, opts)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second, err := manager.Execute(context.Background(), []string{"openai"}, cliproxyexecutor.Request{Model: model}, opts)
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}

	if string(first.Payload) != string(second.Payload) {
		t.Fatalf("sticky routing mismatch: first=%q second=%q", string(first.Payload), string(second.Payload))
	}
	calls := executor.Calls()
	if len(calls) != 2 {
		t.Fatalf("calls = %v, want 2 entries", calls)
	}
	if calls[0] != calls[1] {
		t.Fatalf("expected second request to stick to first auth, got %v", calls)
	}
}

func TestManagerSessionAffinityFallsBackWhenStickyAuthIsSaturated(t *testing.T) {
	t.Parallel()

	const model = "gpt-4.1"
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &affinityExecutor{}
	manager.RegisterExecutor(executor)

	registerAffinityTestAuths(t, manager, model,
		&Auth{
			ID:       "auth-a",
			Provider: "openai",
			Status:   StatusActive,
			Metadata: map[string]any{"concurrency_limit": 1},
		},
		&Auth{ID: "auth-b", Provider: "openai", Status: StatusActive},
	)

	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SessionAffinityMetadataKey: "session-1",
		},
	}

	first, err := manager.Execute(context.Background(), []string{"openai"}, cliproxyexecutor.Request{Model: model}, opts)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	manager.recordAuth429(string(first.Payload), time.Now())
	manager.recordAuth429(string(first.Payload), time.Now())

	second, err := manager.Execute(context.Background(), []string{"openai"}, cliproxyexecutor.Request{Model: model}, opts)
	if err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}
	if string(first.Payload) == string(second.Payload) {
		t.Fatalf("expected saturated sticky auth to be bypassed, both calls used %q", string(first.Payload))
	}
}

func TestPrepareCandidatesForSelection_UsesConfiguredWeights(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	candidates := []*Auth{
		{ID: "a", Attributes: map[string]string{"weight": "3"}},
		{ID: "b", Attributes: map[string]string{"weight": "1"}},
	}

	prepared, opts := manager.prepareCandidatesForSelection(candidates, cliproxyexecutor.Options{})
	if len(prepared) != 2 {
		t.Fatalf("prepared candidates = %d, want 2", len(prepared))
	}
	if got := prepared[0].Attributes["priority"]; got != "3" {
		t.Fatalf("prepared[0] priority = %q, want %q", got, "3")
	}
	if got := prepared[1].Attributes["priority"]; got != "1" {
		t.Fatalf("prepared[1] priority = %q, want %q", got, "1")
	}
	if forced, _ := opts.Metadata[cliproxyexecutor.ForceWeightedSelectionMetadataKey].(bool); !forced {
		t.Fatal("expected weighted selection to be forced")
	}
}

func TestPrepareCandidatesForSelection_PrefersLowerLatencyForNonStreaming(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.recordAuthExperience("fast", false, cliproxyusage.Record{AuthID: "fast", LatencyMs: 120})
	manager.recordAuthExperience("slow", false, cliproxyusage.Record{AuthID: "slow", LatencyMs: 800})

	candidates := []*Auth{
		{ID: "fast"},
		{ID: "slow"},
	}

	prepared, opts := manager.prepareCandidatesForSelection(candidates, cliproxyexecutor.Options{})
	if forced, _ := opts.Metadata[cliproxyexecutor.ForceWeightedSelectionMetadataKey].(bool); !forced {
		t.Fatal("expected weighted selection to be forced by experience metrics")
	}
	got := map[string]string{}
	for _, candidate := range prepared {
		got[candidate.ID] = candidate.Attributes["priority"]
	}
	fastWeight, _ := strconv.Atoi(got["fast"])
	slowWeight, _ := strconv.Atoi(got["slow"])
	if fastWeight <= slowWeight {
		t.Fatalf("expected lower latency auth to get higher effective weight, got fast=%s slow=%s", got["fast"], got["slow"])
	}
}

func TestPrepareCandidatesForSelection_PrefersLowerFirstTokenAndHigherThroughputForStreaming(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.recordAuthExperience("responsive", true, cliproxyusage.Record{
		AuthID:       "responsive",
		LatencyMs:    1400,
		FirstTokenMs: 120,
		Detail: cliproxyusage.Detail{
			OutputTokens: 900,
			TotalTokens:  900,
		},
	})
	manager.recordAuthExperience("sluggish", true, cliproxyusage.Record{
		AuthID:       "sluggish",
		LatencyMs:    2200,
		FirstTokenMs: 700,
		Detail: cliproxyusage.Detail{
			OutputTokens: 200,
			TotalTokens:  200,
		},
	})

	candidates := []*Auth{
		{ID: "responsive"},
		{ID: "sluggish"},
	}

	prepared, _ := manager.prepareCandidatesForSelection(candidates, cliproxyexecutor.Options{Stream: true})
	got := map[string]string{}
	for _, candidate := range prepared {
		got[candidate.ID] = candidate.Attributes["priority"]
	}
	responsiveWeight, _ := strconv.Atoi(got["responsive"])
	sluggishWeight, _ := strconv.Atoi(got["sluggish"])
	if responsiveWeight <= sluggishWeight {
		t.Fatalf("expected stronger streaming auth to get higher effective weight, got responsive=%s sluggish=%s", got["responsive"], got["sluggish"])
	}
}

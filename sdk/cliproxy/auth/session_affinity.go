package auth

import (
	"context"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

const (
	sessionAffinityTTL         = 2 * time.Hour
	loadTrackerWindow          = time.Minute
	recent429Window            = time.Minute
	recent429SoftCapacity      = 2
	defaultSessionBindingLimit = 4096
	experienceEWMAAlpha        = 0.35
)

type sessionBinding struct {
	AuthID     string
	UpdatedAt  time.Time
	LastUsedAt time.Time
}

type tokenUsageEvent struct {
	at     time.Time
	tokens int64
}

type authLoadState struct {
	inflight  int
	rpm       []time.Time
	tpm       []tokenUsageEvent
	recent429 []time.Time
}

type authLoadSnapshot struct {
	inflight  int
	rpm       int
	tpm       int64
	recent429 int
}

type authExperienceStats struct {
	avgLatencyMs    float64
	avgFirstTokenMs float64
	avgThroughput   float64
	samples         int
}

type authExperienceState struct {
	streaming    authExperienceStats
	nonStreaming authExperienceStats
}

type authExperienceSnapshot struct {
	streaming    authExperienceStats
	nonStreaming authExperienceStats
}

type authCapacityLimits struct {
	concurrency int
	rpm         int
	tpm         int
}

type authLoadUsagePlugin struct{}

var (
	authLoadPluginOnce sync.Once
	authLoadManagers   sync.Map
)

func registerAuthLoadManager(m *Manager) {
	if m == nil {
		return
	}
	authLoadPluginOnce.Do(func() {
		cliproxyusage.RegisterPlugin(authLoadUsagePlugin{})
	})
	authLoadManagers.Store(m, struct{}{})
}

func (authLoadUsagePlugin) HandleUsage(ctx context.Context, record cliproxyusage.Record) {
	if strings.TrimSpace(record.AuthID) == "" {
		return
	}
	streaming := requestStreamingFromContext(ctx)
	authLoadManagers.Range(func(key, _ any) bool {
		manager, ok := key.(*Manager)
		if ok && manager != nil {
			manager.recordAuthUsage(record.AuthID, record.Detail.TotalTokens)
			manager.recordAuthExperience(record.AuthID, streaming, record)
		}
		return true
	})
}

type requestStreamingContextKey struct{}

func withRequestStreamingContext(ctx context.Context, streaming bool) context.Context {
	return context.WithValue(ctx, requestStreamingContextKey{}, streaming)
}

func requestStreamingFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(requestStreamingContextKey{})
	streaming, _ := raw.(bool)
	return streaming
}

func sessionAffinityKeyFromMetadata(meta map[string]any) string {
	if len(meta) == 0 {
		return ""
	}
	raw, ok := meta[cliproxyexecutor.SessionAffinityMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch val := raw.(type) {
	case string:
		return strings.TrimSpace(val)
	case []byte:
		return strings.TrimSpace(string(val))
	default:
		return ""
	}
}

func requestedModelFromMetadata(meta map[string]any) string {
	if len(meta) == 0 {
		return ""
	}
	raw, ok := meta[cliproxyexecutor.RequestedModelMetadataKey]
	if !ok || raw == nil {
		return ""
	}
	switch val := raw.(type) {
	case string:
		return strings.TrimSpace(val)
	case []byte:
		return strings.TrimSpace(string(val))
	default:
		return ""
	}
}

func sessionBindingLookupKey(meta map[string]any, fallbackModel string) string {
	sessionKey := sessionAffinityKeyFromMetadata(meta)
	if sessionKey == "" {
		return ""
	}
	modelKey := requestedModelFromMetadata(meta)
	if modelKey == "" {
		modelKey = strings.TrimSpace(fallbackModel)
	}
	if modelKey == "" {
		return sessionKey
	}
	return sessionKey + "|" + modelKey
}

func (m *Manager) rememberSessionAffinity(meta map[string]any, fallbackModel string, authID string, now time.Time) {
	if m == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	key := sessionBindingLookupKey(meta, fallbackModel)
	if key == "" {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.loadMu.Lock()
	if len(m.sessionBindings) >= defaultSessionBindingLimit {
		m.evictOldestSessionBindingLocked(now)
	}
	m.sessionBindings[key] = sessionBinding{AuthID: authID, UpdatedAt: now, LastUsedAt: now}
	m.loadMu.Unlock()
}

func (m *Manager) stickyAuthID(meta map[string]any, fallbackModel string, now time.Time) string {
	if m == nil {
		return ""
	}
	key := sessionBindingLookupKey(meta, fallbackModel)
	if key == "" {
		return ""
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.loadMu.Lock()
	defer m.loadMu.Unlock()
	binding, ok := m.sessionBindings[key]
	if !ok {
		return ""
	}
	if now.Sub(binding.UpdatedAt) > sessionAffinityTTL {
		delete(m.sessionBindings, key)
		return ""
	}
	binding.LastUsedAt = now
	m.sessionBindings[key] = binding
	return strings.TrimSpace(binding.AuthID)
}

func (m *Manager) evictOldestSessionBindingLocked(now time.Time) {
	if len(m.sessionBindings) == 0 {
		return
	}
	var (
		oldestKey string
		oldestAt  time.Time
	)
	for key, binding := range m.sessionBindings {
		if now.Sub(binding.UpdatedAt) > sessionAffinityTTL {
			delete(m.sessionBindings, key)
			continue
		}
		candidate := binding.LastUsedAt
		if candidate.IsZero() {
			candidate = binding.UpdatedAt
		}
		if oldestKey == "" || candidate.Before(oldestAt) {
			oldestKey = key
			oldestAt = candidate
		}
	}
	if oldestKey != "" && len(m.sessionBindings) >= defaultSessionBindingLimit {
		delete(m.sessionBindings, oldestKey)
	}
}

func (m *Manager) beginAuthRequest(authID string, now time.Time) {
	if m == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.loadMu.Lock()
	state := m.authLoadStateLocked(authID, now)
	state.inflight++
	state.rpm = append(state.rpm, now)
	m.loadMu.Unlock()
}

func (m *Manager) finishAuthRequest(authID string) {
	if m == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	m.loadMu.Lock()
	state := m.authLoads[authID]
	if state != nil && state.inflight > 0 {
		state.inflight--
	}
	m.loadMu.Unlock()
}

func (m *Manager) recordAuthUsage(authID string, totalTokens int64) {
	if m == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" || totalTokens <= 0 {
		return
	}
	now := time.Now()
	m.loadMu.Lock()
	state := m.authLoadStateLocked(authID, now)
	state.tpm = append(state.tpm, tokenUsageEvent{at: now, tokens: totalTokens})
	m.loadMu.Unlock()
}

func (m *Manager) recordAuth429(authID string, now time.Time) {
	if m == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.loadMu.Lock()
	state := m.authLoadStateLocked(authID, now)
	state.recent429 = append(state.recent429, now)
	m.loadMu.Unlock()
}

func (m *Manager) authLoadStateLocked(authID string, now time.Time) *authLoadState {
	if m.authLoads == nil {
		m.authLoads = make(map[string]*authLoadState)
	}
	state := m.authLoads[authID]
	if state == nil {
		state = &authLoadState{}
		m.authLoads[authID] = state
	}
	pruneAuthLoadState(state, now)
	return state
}

func pruneAuthLoadState(state *authLoadState, now time.Time) {
	if state == nil {
		return
	}
	rpmCutoff := now.Add(-loadTrackerWindow)
	rpm := state.rpm[:0]
	for _, at := range state.rpm {
		if at.After(rpmCutoff) {
			rpm = append(rpm, at)
		}
	}
	state.rpm = rpm

	tpmCutoff := now.Add(-loadTrackerWindow)
	tpm := state.tpm[:0]
	for _, event := range state.tpm {
		if event.at.After(tpmCutoff) {
			tpm = append(tpm, event)
		}
	}
	state.tpm = tpm

	retryCutoff := now.Add(-recent429Window)
	recent := state.recent429[:0]
	for _, at := range state.recent429 {
		if at.After(retryCutoff) {
			recent = append(recent, at)
		}
	}
	state.recent429 = recent
}

func (m *Manager) authLoadSnapshot(authID string, now time.Time) authLoadSnapshot {
	if m == nil {
		return authLoadSnapshot{}
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return authLoadSnapshot{}
	}
	if now.IsZero() {
		now = time.Now()
	}
	m.loadMu.Lock()
	defer m.loadMu.Unlock()
	state := m.authLoadStateLocked(authID, now)
	var totalTokens int64
	for _, event := range state.tpm {
		totalTokens += event.tokens
	}
	return authLoadSnapshot{
		inflight:  state.inflight,
		rpm:       len(state.rpm),
		tpm:       totalTokens,
		recent429: len(state.recent429),
	}
}

func (m *Manager) recordAuthExperience(authID string, streaming bool, record cliproxyusage.Record) {
	if m == nil || strings.TrimSpace(authID) == "" || record.Failed {
		return
	}
	m.loadMu.Lock()
	defer m.loadMu.Unlock()
	if m.authExperience == nil {
		m.authExperience = make(map[string]*authExperienceState)
	}
	state := m.authExperience[authID]
	if state == nil {
		state = &authExperienceState{}
		m.authExperience[authID] = state
	}
	stats := &state.nonStreaming
	if streaming {
		stats = &state.streaming
	}
	updateEWMA(&stats.avgLatencyMs, float64(maxInt64(record.LatencyMs, 0)), &stats.samples)
	if streaming {
		firstToken := record.FirstTokenMs
		if firstToken <= 0 {
			firstToken = record.LatencyMs
		}
		updateEWMA(&stats.avgFirstTokenMs, float64(maxInt64(firstToken, 0)), nil)
		if throughput := throughputTokensPerSecond(record); throughput > 0 {
			updateEWMA(&stats.avgThroughput, throughput, nil)
		}
	}
}

func (m *Manager) authExperienceSnapshot(authID string) authExperienceSnapshot {
	if m == nil || strings.TrimSpace(authID) == "" {
		return authExperienceSnapshot{}
	}
	m.loadMu.Lock()
	defer m.loadMu.Unlock()
	if m.authExperience == nil {
		return authExperienceSnapshot{}
	}
	state := m.authExperience[authID]
	if state == nil {
		return authExperienceSnapshot{}
	}
	return authExperienceSnapshot{
		streaming:    state.streaming,
		nonStreaming: state.nonStreaming,
	}
}

func updateEWMA(target *float64, value float64, samples *int) {
	if target == nil || value < 0 {
		return
	}
	if samples != nil {
		*samples = *samples + 1
	}
	if *target == 0 {
		*target = value
		return
	}
	*target = (*target * (1 - experienceEWMAAlpha)) + (value * experienceEWMAAlpha)
}

func throughputTokensPerSecond(record cliproxyusage.Record) float64 {
	outputTokens := record.Detail.OutputTokens
	if outputTokens <= 0 {
		outputTokens = record.Detail.TotalTokens - record.Detail.InputTokens
	}
	if outputTokens <= 0 {
		return 0
	}
	durationMs := record.LatencyMs - record.FirstTokenMs
	if durationMs <= 0 {
		durationMs = record.LatencyMs
	}
	if durationMs <= 0 {
		return 0
	}
	return float64(outputTokens) / (float64(durationMs) / 1000.0)
}

func maxInt64(v, floor int64) int64 {
	if v < floor {
		return floor
	}
	return v
}

func authCapacityLimitsFromAuth(auth *Auth) authCapacityLimits {
	return authCapacityLimits{
		concurrency: authLimitInt(auth, "concurrency_limit", "concurrency-limit"),
		rpm:         authLimitInt(auth, "rpm_limit", "rpm-limit"),
		tpm:         authLimitInt(auth, "tpm_limit", "tpm-limit"),
	}
}

func authConfiguredWeight(auth *Auth) int {
	weight, ok := authSelectionWeightValue(auth)
	if !ok {
		return 1
	}
	if weight <= 0 {
		return 0
	}
	return weight
}

func authLimitInt(auth *Auth, keys ...string) int {
	if auth == nil {
		return 0
	}
	for _, key := range keys {
		if auth.Metadata != nil {
			if value, ok := auth.Metadata[key]; ok {
				if parsed, okParsed := parseIntAny(value); okParsed && parsed > 0 {
					return parsed
				}
			}
		}
		if auth.Attributes != nil {
			if value, ok := auth.Attributes[key]; ok {
				if parsed, okParsed := parseIntAny(value); okParsed && parsed > 0 {
					return parsed
				}
			}
		}
	}
	return 0
}

func (m *Manager) authSaturated(auth *Auth, now time.Time) bool {
	if auth == nil {
		return true
	}
	snapshot := m.authLoadSnapshot(auth.ID, now)
	limits := authCapacityLimitsFromAuth(auth)
	if limits.concurrency > 0 && snapshot.inflight >= limits.concurrency {
		return true
	}
	if limits.rpm > 0 && snapshot.rpm >= limits.rpm {
		return true
	}
	if limits.tpm > 0 && snapshot.tpm >= int64(limits.tpm) {
		return true
	}
	return snapshot.recent429 >= recent429SoftCapacity
}

func (m *Manager) filterCandidatesByLoad(candidates []*Auth, enforcePinned bool, now time.Time) []*Auth {
	if enforcePinned || len(candidates) <= 1 {
		return candidates
	}
	filtered := make([]*Auth, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || m.authSaturated(candidate, now) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	if len(filtered) > 0 {
		return filtered
	}
	return candidates
}

func (m *Manager) findStickyCandidate(candidates []*Auth, meta map[string]any, fallbackModel string, now time.Time) *Auth {
	if len(candidates) == 0 {
		return nil
	}
	stickyID := m.stickyAuthID(meta, fallbackModel, now)
	if stickyID == "" {
		return nil
	}
	for _, candidate := range candidates {
		if candidate == nil || candidate.ID != stickyID {
			continue
		}
		if m.authSaturated(candidate, now) {
			return nil
		}
		return candidate
	}
	return nil
}

func (m *Manager) prepareCandidatesForSelection(candidates []*Auth, opts cliproxyexecutor.Options) ([]*Auth, cliproxyexecutor.Options) {
	if len(candidates) <= 1 {
		return candidates, opts
	}
	streaming := opts.Stream
	effective := make([]int, len(candidates))
	shouldForceWeighted := false
	hasExplicitWeight := false
	for i, candidate := range candidates {
		baseWeight := authConfiguredWeight(candidate)
		if baseWeight <= 0 {
			baseWeight = 1
		}
		if configured, ok := authSelectionWeightValue(candidate); ok {
			hasExplicitWeight = true
			if configured > 1 || configured == 0 {
				shouldForceWeighted = true
			}
		}
		effective[i] = baseWeight
	}
	if !hasExplicitWeight {
		for i := range effective {
			effective[i] = 100
		}
	}

	scoreSet := m.experienceScoreSet(candidates, streaming)
	for i, candidate := range candidates {
		baseWeight := effective[i]
		score, ok := scoreSet[candidate.ID]
		if !ok {
			continue
		}
		multiplier := 0.8 + (0.4 * score)
		adjusted := int(math.Round(float64(baseWeight) * multiplier))
		if adjusted < 1 {
			adjusted = 1
		}
		if adjusted != baseWeight {
			shouldForceWeighted = true
		}
		effective[i] = adjusted
	}

	if !shouldForceWeighted && !hasExplicitWeight {
		return candidates, opts
	}

	prepared := make([]*Auth, 0, len(candidates))
	for i, candidate := range candidates {
		cloned := candidate.Clone()
		if cloned == nil {
			continue
		}
		if cloned.Attributes == nil {
			cloned.Attributes = make(map[string]string)
		}
		cloned.Attributes["priority"] = strconv.Itoa(effective[i])
		prepared = append(prepared, cloned)
	}
	if len(prepared) == 0 {
		return candidates, opts
	}
	opts.Metadata = cloneMetadataWithFlag(opts.Metadata, cliproxyexecutor.ForceWeightedSelectionMetadataKey, true)
	return prepared, opts
}

func cloneMetadataWithFlag(meta map[string]any, key string, value any) map[string]any {
	cloned := make(map[string]any, len(meta)+1)
	for k, v := range meta {
		cloned[k] = v
	}
	cloned[key] = value
	return cloned
}

func (m *Manager) experienceScoreSet(candidates []*Auth, streaming bool) map[string]float64 {
	if len(candidates) == 0 {
		return nil
	}
	type metrics struct {
		firstToken float64
		throughput float64
		latency    float64
		hasFT      bool
		hasTP      bool
		hasLatency bool
	}
	statsByID := make(map[string]metrics, len(candidates))
	var (
		minFT, maxFT             float64
		minTP, maxTP             float64
		minLatency, maxLatency   float64
		hasFTSeries, hasTPSeries bool
		hasLatSeries             bool
	)
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		snapshot := m.authExperienceSnapshot(candidate.ID)
		entry := metrics{}
		if streaming {
			if snapshot.streaming.avgFirstTokenMs > 0 {
				entry.firstToken = snapshot.streaming.avgFirstTokenMs
				entry.hasFT = true
				if !hasFTSeries || entry.firstToken < minFT {
					minFT = entry.firstToken
				}
				if !hasFTSeries || entry.firstToken > maxFT {
					maxFT = entry.firstToken
				}
				hasFTSeries = true
			}
			if snapshot.streaming.avgThroughput > 0 {
				entry.throughput = snapshot.streaming.avgThroughput
				entry.hasTP = true
				if !hasTPSeries || entry.throughput < minTP {
					minTP = entry.throughput
				}
				if !hasTPSeries || entry.throughput > maxTP {
					maxTP = entry.throughput
				}
				hasTPSeries = true
			}
		} else if snapshot.nonStreaming.avgLatencyMs > 0 {
			entry.latency = snapshot.nonStreaming.avgLatencyMs
			entry.hasLatency = true
			if !hasLatSeries || entry.latency < minLatency {
				minLatency = entry.latency
			}
			if !hasLatSeries || entry.latency > maxLatency {
				maxLatency = entry.latency
			}
			hasLatSeries = true
		}
		statsByID[candidate.ID] = entry
	}

	if (!streaming && !hasLatSeries) || (streaming && !hasFTSeries && !hasTPSeries) {
		return nil
	}

	scores := make(map[string]float64, len(statsByID))
	for id, entry := range statsByID {
		if streaming {
			ftScore := 0.5
			tpScore := 0.5
			if entry.hasFT {
				ftScore = normalizeLowerBetter(entry.firstToken, minFT, maxFT)
			}
			if entry.hasTP {
				tpScore = normalizeHigherBetter(entry.throughput, minTP, maxTP)
			}
			scores[id] = (0.65 * ftScore) + (0.35 * tpScore)
			continue
		}
		if entry.hasLatency {
			scores[id] = normalizeLowerBetter(entry.latency, minLatency, maxLatency)
		}
	}
	return scores
}

func normalizeLowerBetter(value, minValue, maxValue float64) float64 {
	if maxValue <= minValue {
		return 0.5
	}
	score := (maxValue - value) / (maxValue - minValue)
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}

func normalizeHigherBetter(value, minValue, maxValue float64) float64 {
	if maxValue <= minValue {
		return 0.5
	}
	score := (value - minValue) / (maxValue - minValue)
	if score < 0 {
		return 0
	}
	if score > 1 {
		return 1
	}
	return score
}

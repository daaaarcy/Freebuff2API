package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	freeAgentsSourceURL  = "https://raw.githubusercontent.com/CodebuffAI/codebuff/main/common/src/constants/free-agents.ts"
	freeModelsSourceURL  = "https://raw.githubusercontent.com/CodebuffAI/codebuff/main/common/src/constants/freebuff-models.ts"
	modelRefreshInterval = 6 * time.Hour
)

// hardcodedFallback is used when the remote fetch fails on startup.
var hardcodedFallback = map[string][]string{
	"base2-free":              {"minimax/minimax-m2.7", "deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash", "moonshotai/kimi-k2.6"},
	"base2-free-kimi":         {"moonshotai/kimi-k2.6"},
	"base2-free-deepseek":     {"deepseek/deepseek-v4-pro"},
	"base2-free-deepseek-flash": {"deepseek/deepseek-v4-flash"},
	"file-picker":             {"google/gemini-2.5-flash-lite"},
	"file-picker-max":         {"google/gemini-3.1-flash-lite-preview"},
	"file-lister":             {"google/gemini-3.1-flash-lite-preview"},
	"researcher-web":          {"google/gemini-3.1-flash-lite-preview"},
	"researcher-docs":         {"google/gemini-3.1-flash-lite-preview"},
	"basher":                  {"google/gemini-3.1-flash-lite-preview"},
	"code-reviewer-minimax":   {"minimax/minimax-m2.7"},
	"code-reviewer-kimi":      {"moonshotai/kimi-k2.6"},
	"code-reviewer-deepseek":  {"deepseek/deepseek-v4-pro"},
	"code-reviewer-deepseek-flash": {"deepseek/deepseek-v4-flash"},
	"code-reviewer-lite":      {"minimax/minimax-m2.7", "moonshotai/kimi-k2.6", "deepseek/deepseek-v4-pro", "deepseek/deepseek-v4-flash"},
}

// ModelRegistry fetches and caches the agent→model mapping for all free agents
// from the upstream free-agents.ts source file.
type ModelRegistry struct {
	client *http.Client
	logger *log.Logger

	mu           sync.RWMutex
	agentModels  map[string][]string // agentID → []model
	modelToAgent map[string]string   // model → chosen agentID
	allModels    []string            // deduplicated, sorted
	lastOK       time.Time

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewModelRegistry(client *http.Client, logger *log.Logger) *ModelRegistry {
	return &ModelRegistry{
		client:       client,
		logger:       logger,
		agentModels:  make(map[string][]string),
		modelToAgent: make(map[string]string),
		stopCh:       make(chan struct{}),
	}
}

func (r *ModelRegistry) Start(ctx context.Context) {
	if err := r.refresh(ctx); err != nil {
		r.logger.Printf("model registry: initial fetch failed, loading hardcoded fallback: %v", err)
		r.loadFallback()
	}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(modelRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				if err := r.refresh(ctx); err != nil {
					r.logger.Printf("model registry: refresh failed: %v", err)
				}
				cancel()
			case <-r.stopCh:
				return
			}
		}
	}()
}

func (r *ModelRegistry) Stop() {
	close(r.stopCh)
	r.wg.Wait()
}

// Models returns the deduplicated list of all available model names.
func (r *ModelRegistry) Models() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.allModels))
	copy(out, r.allModels)
	return out
}

// HasModel checks if the given model is available.
func (r *ModelRegistry) HasModel(model string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.modelToAgent[model]
	return ok
}

// AgentForModel returns the agent ID that should serve the given model.
func (r *ModelRegistry) AgentForModel(model string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	agent, ok := r.modelToAgent[model]
	return agent, ok
}

// AgentIDs returns the list of all known agent IDs.
func (r *ModelRegistry) AgentIDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.agentModels))
	for id := range r.agentModels {
		ids = append(ids, id)
	}
	return ids
}

func (r *ModelRegistry) refresh(ctx context.Context) error {
	agentsSrc, err := r.fetchSource(ctx, freeAgentsSourceURL)
	if err != nil {
		return fmt.Errorf("fetch free-agents source: %w", err)
	}

	modelsSrc, err := r.fetchSource(ctx, freeModelsSourceURL)
	if err != nil {
		return fmt.Errorf("fetch freebuff-models source: %w", err)
	}

	constants := parseModelConstants(modelsSrc)

	// Update root agent IDs from upstream source
	if parsed := parseRootAgentIDs(agentsSrc); len(parsed) > 0 {
		rootAgentIDs = parsed
	}

	all := parseAllFreeModels(agentsSrc, constants)
	if len(all) == 0 {
		return fmt.Errorf("no free agents found in source")
	}

	modelToAgent, allModels := buildModelMapping(all)

	r.mu.Lock()
	r.agentModels = all
	r.modelToAgent = modelToAgent
	r.allModels = allModels
	r.lastOK = time.Now()
	r.mu.Unlock()

	r.logger.Printf("model registry: updated %d agents, %d models: %v", len(all), len(allModels), allModels)
	return nil
}

func (r *ModelRegistry) fetchSource(ctx context.Context, url string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := r.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	return string(body), nil
}

func (r *ModelRegistry) loadFallback() {
	modelToAgent, allModels := buildModelMapping(hardcodedFallback)

	r.mu.Lock()
	r.agentModels = hardcodedFallback
	r.modelToAgent = modelToAgent
	r.allModels = allModels
	r.mu.Unlock()

	r.logger.Printf("model registry: loaded fallback models: %v", allModels)
}

// parseModelConstants extracts constant string assignments from freebuff-models.ts
// e.g. export const FREEBUFF_MINIMAX_MODEL_ID = 'minimax/minimax-m2.7'
func parseModelConstants(source string) map[string]string {
	pattern := regexp.MustCompile(`export\s+const\s+(\w+)\s*=\s*'([^']+)'`)
	constants := make(map[string]string)
	for _, match := range pattern.FindAllStringSubmatch(source, -1) {
		constants[match[1]] = match[2]
	}
	return constants
}

// parseRootAgentIDs extracts root agent IDs from the FREEBUFF_ROOT_AGENT_IDS array in free-agents.ts.
func parseRootAgentIDs(source string) map[string]bool {
	// Match: export const FREEBUFF_ROOT_AGENT_IDS = [ ... ]
	re := regexp.MustCompile(`FREEBUFF_ROOT_AGENT_IDS\s*=\s*\[([^\]]*)\]`)
	m := re.FindStringSubmatch(source)
	if len(m) < 2 {
		return nil
	}
	idPattern := regexp.MustCompile(`'([^']+)'`)
	ids := make(map[string]bool)
	for _, match := range idPattern.FindAllStringSubmatch(m[1], -1) {
		ids[match[1]] = true
	}
	return ids
}

// parseAllFreeModels extracts ALL agent→models mappings from the free-agents.ts source.
// It resolves constant references (e.g. FREEBUFF_MINIMAX_MODEL_ID) using the provided map.
func parseAllFreeModels(source string, constants map[string]string) map[string][]string {
	blockPattern := regexp.MustCompile(`'([^']+)':\s*new\s+Set\(\[([^\]]*)\]\)`)
	stringPattern := regexp.MustCompile(`'([^']+)'`)
	constPattern := regexp.MustCompile(`\b([A-Z][A-Z0-9_]+)\b`)

	result := make(map[string][]string)
	for _, match := range blockPattern.FindAllStringSubmatch(source, -1) {
		agentID := match[1]
		modelsStr := match[2]

		var models []string
		// First try quoted string literals
		for _, modelMatch := range stringPattern.FindAllStringSubmatch(modelsStr, -1) {
			model := strings.TrimSpace(modelMatch[1])
			if model != "" {
				models = append(models, model)
			}
		}
		// Then try constant references
		for _, constMatch := range constPattern.FindAllStringSubmatch(modelsStr, -1) {
			if resolved, ok := constants[constMatch[1]]; ok {
				models = append(models, resolved)
			}
		}
		if len(models) > 0 {
			result[agentID] = models
		}
	}
	return result
}

// rootAgentIDs are agent IDs that can run as top-level free-mode roots.
// Non-root agents (reviewers, subagents) require an active root ancestor run.
// We prefer mapping models to root agents to avoid hierarchy rejections.
var rootAgentIDs = map[string]bool{
	"base2-free":              true,
	"base2-free-kimi":         true,
	"base2-free-deepseek":     true,
	"base2-free-deepseek-flash": true,
}

// buildModelMapping creates the model→agent reverse mapping and deduplicated model list.
// When a model appears in multiple agents, a root agent is preferred.
// Models that can only be served by non-root agents are excluded (they require subagent hierarchy).
func buildModelMapping(agentModels map[string][]string) (map[string]string, []string) {
	modelAgents := make(map[string][]string)
	for agentID, models := range agentModels {
		for _, model := range models {
			modelAgents[model] = append(modelAgents[model], agentID)
		}
	}

	modelToAgent := make(map[string]string, len(modelAgents))
	allModels := make([]string, 0, len(modelAgents))
	for model, agents := range modelAgents {
		// Skip models that have no root agent — they can only run as subagents
		if !hasRootAgent(agents) {
			continue
		}
		modelToAgent[model] = pickAgent(agents)
		allModels = append(allModels, model)
	}
	sort.Strings(allModels)
	return modelToAgent, allModels
}

// hasRootAgent returns true if at least one agent in the list is a root agent.
func hasRootAgent(agents []string) bool {
	for _, a := range agents {
		if rootAgentIDs[a] {
			return true
		}
	}
	return false
}

// pickAgent selects a root agent from the list if available, otherwise picks randomly.
func pickAgent(agents []string) string {
	var roots []string
	for _, a := range agents {
		if rootAgentIDs[a] {
			roots = append(roots, a)
		}
	}
	if len(roots) > 0 {
		return roots[rand.Intn(len(roots))]
	}
	return agents[rand.Intn(len(agents))]
}

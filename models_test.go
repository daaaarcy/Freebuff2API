package main

import "testing"

func TestBuildModelMappingIncludesMiMoRootAgents(t *testing.T) {
	agentModels := map[string][]string{
		"base2-free":          {"minimax/minimax-m2.7"},
		"code-reviewer-mimo":  {"mimo/mimo-v2.5"},
		"base2-free-mimo":     {"mimo/mimo-v2.5"},
		"base2-free-mimo-pro": {"mimo/mimo-v2.5-pro"},
	}

	modelToAgent, allModels := buildModelMapping(agentModels)

	if got := modelToAgent["mimo/mimo-v2.5"]; got != "base2-free-mimo" {
		t.Fatalf("mimo/mimo-v2.5 agent = %q, want base2-free-mimo", got)
	}
	if got := modelToAgent["mimo/mimo-v2.5-pro"]; got != "base2-free-mimo-pro" {
		t.Fatalf("mimo/mimo-v2.5-pro agent = %q, want base2-free-mimo-pro", got)
	}
	if !containsString(allModels, "mimo/mimo-v2.5") {
		t.Fatalf("allModels = %#v, want mimo/mimo-v2.5", allModels)
	}
	if !containsString(allModels, "mimo/mimo-v2.5-pro") {
		t.Fatalf("allModels = %#v, want mimo/mimo-v2.5-pro", allModels)
	}
}

func TestMergeBuiltInFreebuffModelsAddsExecutableDiscoveredMiMoAgents(t *testing.T) {
	agentModels := map[string][]string{
		"base2-free": {"minimax/minimax-m2.7"},
	}

	merged := mergeBuiltInFreebuffModels(agentModels)

	if got := merged["base2-free-mimo"]; len(got) != 1 || got[0] != "mimo/mimo-v2.5" {
		t.Fatalf("base2-free-mimo models = %#v, want mimo/mimo-v2.5", got)
	}
	if got := merged["base2-free-mimo-pro"]; len(got) != 1 || got[0] != "mimo/mimo-v2.5-pro" {
		t.Fatalf("base2-free-mimo-pro models = %#v, want mimo/mimo-v2.5-pro", got)
	}
	if got := merged["code-reviewer-mimo"]; len(got) != 1 || got[0] != "mimo/mimo-v2.5" {
		t.Fatalf("code-reviewer-mimo models = %#v, want mimo/mimo-v2.5", got)
	}
	if got := agentModels["base2-free-mimo"]; len(got) != 0 {
		t.Fatalf("input map mutated with base2-free-mimo = %#v", got)
	}
}

func TestMergeBuiltInRootAgentIDsAddsMiMoRoots(t *testing.T) {
	roots := map[string]bool{
		"base2-free": true,
	}

	merged := mergeBuiltInRootAgentIDs(roots)

	if !merged["base2-free-mimo"] {
		t.Fatalf("merged roots = %#v, want base2-free-mimo", merged)
	}
	if !merged["base2-free-mimo-pro"] {
		t.Fatalf("merged roots = %#v, want base2-free-mimo-pro", merged)
	}
	if roots["base2-free-mimo"] {
		t.Fatalf("input roots mutated: %#v", roots)
	}
}

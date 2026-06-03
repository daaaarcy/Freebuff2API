package main

import "testing"

func TestDefaultFreeSessionModelsIncludeMiMo(t *testing.T) {
	for _, model := range []string{
		"mimo/mimo-v2.5",
		"mimo/mimo-v2.5-pro",
	} {
		if !containsString(defaultSessionRequiredModels, model) {
			t.Fatalf("defaultSessionRequiredModels = %#v, want %s", defaultSessionRequiredModels, model)
		}
	}
}

func TestDefaultPremiumSessionModelsIncludeMiMoPro(t *testing.T) {
	if !containsString(defaultPremiumSessionModels, "mimo/mimo-v2.5-pro") {
		t.Fatalf("defaultPremiumSessionModels = %#v, want mimo/mimo-v2.5-pro", defaultPremiumSessionModels)
	}
	if containsString(defaultPremiumSessionModels, "mimo/mimo-v2.5") {
		t.Fatalf("defaultPremiumSessionModels = %#v, did not expect mimo/mimo-v2.5", defaultPremiumSessionModels)
	}
}

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

func TestDefaultSessionTransitionPeriodIsTenMinutes(t *testing.T) {
	t.Setenv("AUTH_TOKENS", "token")

	cfg, err := loadConfig("")
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got, want := cfg.SessionTransitionPeriod.String(), "10m0s"; got != want {
		t.Fatalf("SessionTransitionPeriod = %s, want %s", got, want)
	}
}

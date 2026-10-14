package config

import "testing"

// TestWorkerRejectsInvalidConfig checks that invalid failure probabilities and delay
// bounds are rejected.
func TestWorkerRejectsInvalidConfig(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"FAIL_RATE", "NaN"}, {"FAIL_RATE", "+Inf"}, {"FAIL_RATE", "1.1"},
		{"FAIL_RATE", "-0.1"}, {"MIN_DELAY_MS", "-1"}, {"MAX_DELAY_MS", "-1"},
		{"MAX_DELAY_MS", "10"},
	} {
		t.Run(tc.key+tc.value, func(t *testing.T) {
			t.Setenv("FAIL_RATE", "0")
			t.Setenv("MIN_DELAY_MS", "100")
			t.Setenv("MAX_DELAY_MS", "200")
			t.Setenv(tc.key, tc.value)
			if _, err := LoadWorker(); err == nil {
				t.Fatal("expected invalid config")
			}
		})
	}
}

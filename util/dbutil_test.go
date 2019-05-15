package util

import (
	"testing"
)

func TestSanitizeDBString(t *testing.T) {
	test := []string{"hello", "%", "bobby';DROP TABLE students;"}
	expected := []string{"hello", "%", "bobbyTABLEstudents"}

	for i := range test {
		clean := SanitizeDBString(test[i])
		if clean != expected[i] {
			t.Fatalf("Expected: %s, Got: %s", expected[i], clean)
		}
	}
}

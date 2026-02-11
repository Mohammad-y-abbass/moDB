package lexer

import "testing"

func TestIsDigit(t *testing.T) {
	tests := []struct {
		input    byte
		expected bool
	}{
		{'1', true},
		{'a', false},
		{'0', true},
		{'9', true},
		{'-', false},
	}

	for _, tt := range tests {
		if isDigit(tt.input) != tt.expected {
			t.Errorf("isDigit(%c) = %v; expected %v", tt.input, isDigit(tt.input), tt.expected)
		}
	}
}

func TestIsAlpha(t *testing.T) {
	tests := []struct {
		input    byte
		expected bool
	}{
		{'a', true},
		{'A', true},
		{'z', true},
		{'Z', true},
		{'1', false},
		{'-', false},
	}

	for _, tt := range tests {
		if isAlpha(tt.input) != tt.expected {
			t.Errorf("isAlpha(%c) = %v; expected %v", tt.input, isAlpha(tt.input), tt.expected)
		}
	}
}

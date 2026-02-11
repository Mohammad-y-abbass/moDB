package lexer

import "testing"

func TestAdvance(t *testing.T) {
	l := New("abcde")

	tests := []struct {
		expected byte
		line     int
		col      int
	}{
		{'a', 1, 2},
		{'b', 1, 3},
		{'c', 1, 4},
		{'d', 1, 5},
		{'e', 1, 6},
		{0, 1, 6},
	}

	for i, tt := range tests {
		got := l.advance()

		if got != tt.expected {
			t.Errorf("Step %d: char = %q; expected %q", i, got, tt.expected)
		}
		if l.line != tt.line {
			t.Errorf("Step %d: line = %d; expected %d", i, l.line, tt.line)
		}
		if l.column != tt.col {
			t.Errorf("Step %d: col = %d; expected %d", i, l.column, tt.col)
		}
	}

}

func TestPeek(t *testing.T) {
	l := New("abc")

	if l.peek() != 'a' {
		t.Errorf("peek() = %q; expected %q", l.peek(), 'a')
	}

	l.advance()

	if l.peek() != 'b' {
		t.Errorf("peek() = %q; expected %q", l.peek(), 'b')
	}

	l.advance()

	if l.peek() != 'c' {
		t.Errorf("peek() = %q; expected %q", l.peek(), 'c')
	}

	l.advance()

	if l.peek() != 0 {
		t.Errorf("peek() = %q; expected %q", l.peek(), 0)
	}

}

func TestSkipWhitespace(t *testing.T) {
	l := New("   abc\t\r")

	l.skipWhitespace()

	if l.peek() != 'a' {
		t.Errorf("peek() = %q; expected %q", l.peek(), 'a')
	}

	if l.line != 1 {
		t.Errorf("line = %d; expected %d", l.line, 1)
	}

	if l.column != 4 {
		t.Errorf("column = %d; expected %d", l.column, 4)
	}

}

func TestReadIdentifierTable(t *testing.T) {
	tests := []struct {
		input        string
		expectedType TokenType
		expectedVal  string
	}{
		{"SELECT", SELECT_TOKEN, "SELECT"},
		{"select", SELECT_TOKEN, "select"},
		{"FROM", FROM_TOKEN, "FROM"},
		{"users", IDENTIFIER, "users"},
		{"MyTable", IDENTIFIER, "MyTable"},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.readIdentifier()

		if tok.Type != tt.expectedType {
			t.Errorf("input %q: expected type %v, got %v", tt.input, tt.expectedType, tok.Type)
		}

		if tok.Value != tt.expectedVal {
			t.Errorf("input %q: expected value %q, got %q", tt.input, tt.expectedVal, tok.Value)
		}

		if tok.col != 1 {
			t.Errorf("input %q: expected col 1, got %d", tt.input, tok.col)
		}
	}
}

func TestReadNumber(t *testing.T) {
	tests := []struct {
		input        string
		expectedType TokenType
		expectedVal  string
	}{
		{"123", NUMBER, "123"},
		{"0", NUMBER, "0"},
		{"999", NUMBER, "999"},
		{"-123", NUMBER, "-123"},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.readNumber()

		if tok.Type != tt.expectedType {
			t.Errorf("input %q: expected type %v, got %v", tt.input, tt.expectedType, tok.Type)
		}

		if tok.Value != tt.expectedVal {
			t.Errorf("input %q: expected value %q, got %q", tt.input, tt.expectedVal, tok.Value)
		}

		if tok.col != 1 {
			t.Errorf("input %q: expected col 1, got %d", tt.input, tok.col)
		}
	}
}

func TestNextToken(t *testing.T) {
	tests := []struct {
		input        string
		expectedType TokenType
		expectedVal  string
	}{
		{"SELECT", SELECT_TOKEN, "SELECT"},
		{"select", SELECT_TOKEN, "select"},
		{"FROM", FROM_TOKEN, "FROM"},
		{"users", IDENTIFIER, "users"},
		{"MyTable", IDENTIFIER, "MyTable"},
		{"123", NUMBER, "123"},
		{"0", NUMBER, "0"},
		{"999", NUMBER, "999"},
		{"-", ILLEGAL, "-"},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Errorf("input %q: expected type %v, got %v", tt.input, tt.expectedType, tok.Type)
		}

		if tok.Value != tt.expectedVal {
			t.Errorf("input %q: expected value %q, got %q", tt.input, tt.expectedVal, tok.Value)
		}

		if tok.col != 1 {
			t.Errorf("input %q: expected col 1, got %d", tt.input, tok.col)
		}
	}
}

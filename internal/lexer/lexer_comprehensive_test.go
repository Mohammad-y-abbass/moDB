package lexer

import "testing"

func TestNewLexer(t *testing.T) {
	l := New("test")
	if l.input != "test" || l.cursor != 0 || l.Line != 1 || l.column != 1 {
		t.Errorf("Lexer not initialized properly")
	}
}

func TestAdvanceEmptyInput(t *testing.T) {
	l := New("")
	got := l.advance()
	if got != 0 {
		t.Errorf("expected 0, got %q", got)
	}
	if l.cursor != 0 {
		t.Errorf("cursor should not advance on empty input")
	}
}

func TestPeekEmptyInput(t *testing.T) {
	l := New("")
	if l.peek() != 0 {
		t.Errorf("expected 0, got %q", l.peek())
	}
}

func TestSkipWhitespaceMixed(t *testing.T) {
	l := New("  \t\r\n  abc")
	l.skipWhitespace()
	if l.peek() != 'a' {
		t.Errorf("expected 'a', got %q", l.peek())
	}
	if l.Line != 2 {
		t.Errorf("expected line 2, got %d", l.Line)
	}
	if l.column != 3 {
		t.Errorf("expected column 3, got %d", l.column)
	}
}

func TestSkipWhitespaceOnlyWhitespace(t *testing.T) {
	l := New("   \t\n  ")
	l.skipWhitespace()
	if l.cursor != len(l.input) {
		t.Errorf("expected cursor at end of input")
	}
}

func TestReadIdentifierKeywords(t *testing.T) {
	keywords := []struct {
		input string
		typ   TokenType
	}{
		{"INSERT", INSERT_TOKEN},
		{"UPDATE", UPDATE_TOKEN},
		{"DELETE", DELETE_TOKEN},
		{"WHERE", WHERE_TOKEN},
		{"FROM", FROM_TOKEN},
		{"VALUES", VALUES_TOKEN},
		{"INTO", INTO_TOKEN},
		{"SET", SET_TOKEN},
		{"DEFAULT", DEFAULT_TOKEN},
		{"ORDER", ORDER_TOKEN},
		{"BY", BY_TOKEN},
		{"ASC", ASC_TOKEN},
		{"DESC", DESC_TOKEN},
		{"DISTINCT", DISTINCT_TOKEN},
		{"LIMIT", LIMIT_TOKEN},
		{"OFFSET", OFFSET_TOKEN},
		{"SHOW", SHOW_TOKEN},
		{"DROP", DROP_TOKEN},
		{"CREATE", CREATE_TOKEN},
		{"DATABASE", DATABASE_TOKEN},
		{"USE", USE_TOKEN},
		{"TABLE", TABLE_TOKEN},
		{"INT", INT_TOKEN},
		{"INTEGER", INT_TOKEN},
		{"TEXT", TEXT_TOKEN},
		{"VARCHAR", TEXT_TOKEN},
		{"LIKE", LIKE_TOKEN},
		{"IN", IN_TOKEN},
		{"BETWEEN", BETWEEN_TOKEN},
		{"IS", IS_TOKEN},
		{"AND", AND_TOKEN},
		{"NOT", NOT_TOKEN},
		{"NULL", NULL_TOKEN},
		{"NULL", NULL_TOKEN},
		{"UNIQUE", UNIQUE_TOKEN},
		{"PRIMARY", PRIMARY_TOKEN},
		{"KEY", KEY_TOKEN},
		{"TRUE", TRUE_TOKEN},
		{"FALSE", FALSE_TOKEN},
		{"JOIN", JOIN_TOKEN},
		{"ON", ON_TOKEN},
		{"REFERENCES", REFERENCES_TOKEN},
		{"FOREIGN", FOREIGN_TOKEN},
	}

	for _, kw := range keywords {
		l := New(kw.input)
		tok := l.ReadIdentifier()
		if tok.Type != kw.typ {
			t.Errorf("keyword %q: expected type %v, got %v", kw.input, kw.typ, tok.Type)
		}
		if tok.Value != kw.input {
			t.Errorf("keyword %q: expected value %q, got %q", kw.input, kw.input, tok.Value)
		}
	}
}

func TestReadIdentifierNonKeywords(t *testing.T) {
	tests := []string{
		"my_table",
		"user1",
		"dev@test.com",
		"system.info",
		"my-table",
		"$price",
		"a",
		"Z",
		"_hidden",
		"@variable",
	}

	for _, input := range tests {
		l := New(input)
		tok := l.ReadIdentifier()
		if tok.Type != IDENTIFIER {
			t.Errorf("input %q: expected IDENTIFIER, got %v", input, tok.Type)
		}
		if tok.Value != input {
			t.Errorf("input %q: expected value %q, got %q", input, input, tok.Value)
		}
	}
}

func TestReadNumberEdgeCases(t *testing.T) {
	tests := []struct {
		input        string
		expectedType TokenType
		expectedVal  string
	}{
		{"-0", NUMBER, "-0"},
		{"-999", NUMBER, "-999"},
		{"-", ILLEGAL, "-"},
		{"0", NUMBER, "0"},
		{"00", NUMBER, "00"},
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
	}
}

func TestReadString(t *testing.T) {
	tests := []struct {
		input        string
		expectedType TokenType
		expectedVal  string
	}{
		{"'hello'", STRING, "hello"},
		{"''", STRING, ""},
		{"'test with spaces'", STRING, "test with spaces"},
		{"'123'", STRING, "123"},
		{"'unclosed", ILLEGAL, "unclosed"},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.readString()
		if tok.Type != tt.expectedType {
			t.Errorf("input %q: expected type %v, got %v", tt.input, tt.expectedType, tok.Type)
		}
		if tok.Value != tt.expectedVal {
			t.Errorf("input %q: expected value %q, got %q", tt.input, tt.expectedVal, tok.Value)
		}
	}
}

func TestNextTokenComprehensive(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1 AND name != 'john';"
	l := New(input)

	expected := []struct {
		typ   TokenType
		value string
	}{
		{SELECT_TOKEN, "SELECT"},
		{ASTERISK, "*"},
		{FROM_TOKEN, "FROM"},
		{IDENTIFIER, "users"},
		{WHERE_TOKEN, "WHERE"},
		{IDENTIFIER, "id"},
		{EQ, "="},
		{NUMBER, "1"},
		{AND_TOKEN, "AND"},
		{IDENTIFIER, "name"},
		{NOT_EQ, "!="},
		{STRING, "john"},
		{SEMICOLON, ";"},
		{EOF_TOKEN, ""},
	}

	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("step %d: expected type %v, got %v (value=%q)", i, exp.typ, tok.Type, tok.Value)
		}
		if tok.Value != exp.value {
			t.Errorf("step %d: expected value %q, got %q", i, exp.value, tok.Value)
		}
	}
}

func TestNextTokenOperators(t *testing.T) {
	input := "= != > < >= <= ! ( ) , ; *"
	l := New(input)

	expected := []struct {
		typ   TokenType
		value string
	}{
		{EQ, "="},
		{NOT_EQ, "!="},
		{GT, ">"},
		{LT, "<"},
		{GTE, ">="},
		{LTE, "<="},
		{ILLEGAL, "!"},
		{LPAREN, "("},
		{RPAREN, ")"},
		{COMMA, ","},
		{SEMICOLON, ";"},
		{ASTERISK, "*"},
		{EOF_TOKEN, ""},
	}

	for i, exp := range expected {
		tok := l.NextToken()
		if tok.Type != exp.typ {
			t.Errorf("step %d: expected type %v, got %v (value=%q)", i, exp.typ, tok.Type, tok.Value)
		}
		if tok.Value != exp.value {
			t.Errorf("step %d: expected value %q, got %q", i, exp.value, tok.Value)
		}
	}
}

func TestNextTokenMultipleStatements(t *testing.T) {
	input := "SELECT * FROM t1; INSERT INTO t2 VALUES (1);"
	l := New(input)

	count := 0
	for {
		tok := l.NextToken()
		count++
		if tok.Type == EOF_TOKEN {
			break
		}
	}
	if count <= 2 {
		t.Errorf("expected many tokens, got %d", count)
	}
}

func TestNextTokenIllegalChar(t *testing.T) {
	input := "@"
	l := New(input)
	tok := l.NextToken()
	if tok.Type != IDENTIFIER {
		t.Errorf("expected IDENTIFIER for @, got %v", tok.Type)
	}
}

func TestNextTokenIllegalCharHash(t *testing.T) {
	input := "#"
	l := New(input)
	tok := l.NextToken()
	if tok.Type != ILLEGAL {
		t.Errorf("expected ILLEGAL for #, got %v", tok.Type)
	}
}

func TestNextTokenEmptyInput(t *testing.T) {
	l := New("")
	tok := l.NextToken()
	if tok.Type != EOF_TOKEN {
		t.Errorf("expected EOF_TOKEN, got %v", tok.Type)
	}
}

func TestNextTokenOnlyWhitespace(t *testing.T) {
	l := New("   \t\n  ")
	tok := l.NextToken()
	if tok.Type != EOF_TOKEN {
		t.Errorf("expected EOF_TOKEN, got %v", tok.Type)
	}
}

func TestNextTokenLinePositionTracking(t *testing.T) {
	input := "SELECT\n*\nFROM\nusers"
	l := New(input)

	tests := []struct {
		typ  TokenType
		line int
		col  int
	}{
		{SELECT_TOKEN, 1, 1},
		{ASTERISK, 2, 1},
		{FROM_TOKEN, 3, 1},
		{IDENTIFIER, 4, 1},
	}

	for _, tt := range tests {
		tok := l.NextToken()
		if tok.Type != tt.typ || tok.Line != tt.line || tok.Col != tt.col {
			t.Errorf("expected (%v, line=%d, col=%d), got (%v, line=%d, col=%d)",
				tt.typ, tt.line, tt.col, tok.Type, tok.Line, tok.Col)
		}
	}
}

func TestNextTokenStringLiteral(t *testing.T) {
	input := "'hello world' 'foo'"
	l := New(input)

	tok1 := l.NextToken()
	if tok1.Type != STRING || tok1.Value != "hello world" {
		t.Errorf("expected STRING 'hello world', got %v %q", tok1.Type, tok1.Value)
	}

	tok2 := l.NextToken()
	if tok2.Type != STRING || tok2.Value != "foo" {
		t.Errorf("expected STRING 'foo', got %v %q", tok2.Type, tok2.Value)
	}
}

func TestNextTokenNegativeNumbers(t *testing.T) {
	input := "-123 - 456"
	l := New(input)

	tok1 := l.NextToken()
	if tok1.Type != NUMBER || tok1.Value != "-123" {
		t.Errorf("expected NUMBER -123, got %v %q", tok1.Type, tok1.Value)
	}

	tok2 := l.NextToken()
	if tok2.Type != ILLEGAL || tok2.Value != "-" {
		t.Errorf("expected ILLEGAL '-', got %v %q", tok2.Type, tok2.Value)
	}

	tok3 := l.NextToken()
	if tok3.Type != NUMBER || tok3.Value != "456" {
		t.Errorf("expected NUMBER 456, got %v %q", tok3.Type, tok3.Value)
	}
}

func TestNextTokenJoinStatement(t *testing.T) {
	input := "SELECT * FROM orders JOIN users ON orders.user_id = users.id"
	l := New(input)

	tokens := []TokenType{
		SELECT_TOKEN, ASTERISK, FROM_TOKEN, IDENTIFIER,
		JOIN_TOKEN, IDENTIFIER, ON_TOKEN,
		IDENTIFIER, EQ, IDENTIFIER,
	}

	for i, expectedType := range tokens {
		tok := l.NextToken()
		if tok.Type != expectedType {
			t.Errorf("token %d: expected %v, got %v (%q)", i, expectedType, tok.Type, tok.Value)
		}
	}
}

func TestReadIdentifierKeywordCaseVariants(t *testing.T) {
	tests := []struct {
		input string
		typ   TokenType
	}{
		{"select", SELECT_TOKEN},
		{"Select", SELECT_TOKEN},
		{"SELECT", SELECT_TOKEN},
		{"insert", INSERT_TOKEN},
		{"from", FROM_TOKEN},
		{"where", WHERE_TOKEN},
	}

	for _, tt := range tests {
		l := New(tt.input)
		tok := l.ReadIdentifier()
		if tok.Type != tt.typ {
			t.Errorf("input %q: expected %v, got %v (%q)", tt.input, tt.typ, tok.Type, tok.Value)
		}
	}
}

func TestNextTokenBareSemicolon(t *testing.T) {
	l := New(";")
	tok := l.NextToken()
	if tok.Type != SEMICOLON {
		t.Errorf("expected SEMICOLON, got %v", tok.Type)
	}
	tok = l.NextToken()
	if tok.Type != EOF_TOKEN {
		t.Errorf("expected EOF_TOKEN, got %v", tok.Type)
	}
}

func TestNewInitializesCorrectly(t *testing.T) {
	l := New("hello")
	if l.input != "hello" || l.cursor != 0 || l.column != 1 || l.Line != 1 {
		t.Errorf("New() did not initialize fields correctly")
	}
}

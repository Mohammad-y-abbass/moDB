package lexer

import "strings"

type Lexer struct {
	input  string
	column int
	line   int
	cursor int
}

func New(input string) *Lexer {
	return &Lexer{
		input:  input,
		column: 1,
		line:   1,
		cursor: 0,
	}
}

func (l *Lexer) advance() byte {
	if l.cursor >= len(l.input) {
		return 0
	}

	char := l.input[l.cursor]
	l.cursor++
	l.column++
	return char
}

func (l *Lexer) peek() byte {
	if l.cursor >= len(l.input) {
		return 0
	}
	return l.input[l.cursor]
}

func (l *Lexer) skipWhitespace() {
	for {
		char := l.peek()
		switch char {
		case ' ', '\t', '\r':
			l.advance()
		case '\n':
			l.line++
			l.column = 1
			l.cursor++
		default:
			return
		}
	}
}

func (l *Lexer) readIdentifier() Token {
	start := l.cursor
	startCol := l.column

	for l.cursor < len(l.input) && isAlpha(l.input[l.cursor]) {
		l.advance()
	}

	value := l.input[start:l.cursor]

	upperValue := strings.ToUpper(value)

	switch upperValue {
	case "SELECT":
		return Token{Type: SELECT_TOKEN, Value: value, line: l.line, col: startCol}
	case "FROM":
		return Token{Type: FROM_TOKEN, Value: value, line: l.line, col: startCol}
	case "TRUE":
		return Token{Type: TRUE_TOKEN, Value: value, line: l.line, col: startCol}
	case "FALSE":
		return Token{Type: FALSE_TOKEN, Value: value, line: l.line, col: startCol}
	default:
		return Token{Type: IDENTIFIER, Value: value, line: l.line, col: startCol}
	}
}

func (l *Lexer) readNumber() Token {
	start := l.cursor
	startCol := l.column

	if l.peek() == '-' {
		l.advance()
	}

	for l.cursor < len(l.input) && isDigit(l.peek()) {
		l.advance()
	}

	value := l.input[start:l.cursor]

	return Token{
		Type:  NUMBER,
		Value: value,
		line:  l.line,
		col:   startCol,
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.cursor >= len(l.input) {
		return Token{Type: EOF_TOKEN, Value: "", line: l.line, col: l.column}
	}

	char := l.peek()

	if isAlpha(char) {
		return l.readIdentifier()
	}

	if isDigit(char) {
		return l.readNumber()
	}

	switch char {
	case ',':
		l.advance()
		return Token{Type: COMMA, Value: ",", line: l.line, col: l.column - 1}
	case ';':
		l.advance()
		return Token{Type: SEMICOLON, Value: ";", line: l.line, col: l.column - 1}
	}

	l.advance()
	return Token{Type: ILLEGAL, Value: string(char), line: l.line, col: l.column - 1}
}

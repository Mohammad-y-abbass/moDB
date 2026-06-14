package lexer

import "strings"

type Lexer struct {
	input  string
	column int
	Line   int
	cursor int
}

func New(input string) *Lexer {
	return &Lexer{
		input:  input,
		column: 1,
		Line:   1,
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
			l.Line++
			l.column = 1
			l.cursor++
		default:
			return
		}
	}
}

func (l *Lexer) ReadIdentifier() Token {
	start := l.cursor
	startCol := l.column

	for l.cursor < len(l.input) && isIdentifierPart(l.input[l.cursor]) {
		l.advance()
	}

	value := l.input[start:l.cursor]

	upperValue := strings.ToUpper(value)

	switch upperValue {
	case "SELECT":
		return Token{Type: SELECT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "INSERT":
		return Token{Type: INSERT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "UPDATE":
		return Token{Type: UPDATE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "DELETE":
		return Token{Type: DELETE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "WHERE":
		return Token{Type: WHERE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "FROM":
		return Token{Type: FROM_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "VALUES":
		return Token{Type: VALUES_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "INTO":
		return Token{Type: INTO_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "SET":
		return Token{Type: SET_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "AND":
		return Token{Type: AND_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "LIKE":
		return Token{Type: LIKE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "IN":
		return Token{Type: IN_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "BETWEEN":
		return Token{Type: BETWEEN_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "IS":
		return Token{Type: IS_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "DISTINCT":
		return Token{Type: DISTINCT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "DEFAULT":
		return Token{Type: DEFAULT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "ORDER":
		return Token{Type: ORDER_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "BY":
		return Token{Type: BY_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "ASC":
		return Token{Type: ASC_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "DESC":
		return Token{Type: DESC_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "LIMIT":
		return Token{Type: LIMIT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "OFFSET":
		return Token{Type: OFFSET_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "SHOW":
		return Token{Type: SHOW_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "DROP":
		return Token{Type: DROP_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "CREATE":
		return Token{Type: CREATE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "DATABASE", "DATABASES":
		return Token{Type: DATABASE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "USE":
		return Token{Type: USE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "TABLE", "TABLES":
		return Token{Type: TABLE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "INT", "INTEGER":
		return Token{Type: INT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "TEXT", "VARCHAR":
		return Token{Type: TEXT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "NOT":
		return Token{Type: NOT_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "NULL":
		return Token{Type: NULL_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "UNIQUE":
		return Token{Type: UNIQUE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "PRIMARY":
		return Token{Type: PRIMARY_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "KEY":
		return Token{Type: KEY_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "TRUE":
		return Token{Type: TRUE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "FALSE":
		return Token{Type: FALSE_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "JOIN":
		return Token{Type: JOIN_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "ON":
		return Token{Type: ON_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "REFERENCES":
		return Token{Type: REFERENCES_TOKEN, Value: value, Line: l.Line, Col: startCol}
	case "FOREIGN":
		return Token{Type: FOREIGN_TOKEN, Value: value, Line: l.Line, Col: startCol}
	default:
		return Token{Type: IDENTIFIER, Value: value, Line: l.Line, Col: startCol}
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

	// Use ILLEGAL if it's just a lone dash
	tokenType := NUMBER
	if value == "-" {
		tokenType = ILLEGAL
	}

	return Token{
		Type:  tokenType,
		Value: value,
		Line:  l.Line,
		Col:   startCol,
	}
}

func (l *Lexer) readString() Token {
	startCol := l.column
	l.advance() // Skip the opening quote
	start := l.cursor

	for l.cursor < len(l.input) && l.peek() != '\'' {
		l.advance()
	}

	value := l.input[start:l.cursor]

	if l.cursor >= len(l.input) {
		return Token{Type: ILLEGAL, Value: value, Line: l.Line, Col: startCol}
	}

	l.advance() // Skip the closing quote

	return Token{
		Type:  STRING,
		Value: value,
		Line:  l.Line,
		Col:   startCol,
	}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.cursor >= len(l.input) {
		return Token{Type: EOF_TOKEN, Value: "", Line: l.Line, Col: l.column}
	}

	char := l.peek()

	if isAlpha(char) || char == '.' {
		return l.ReadIdentifier()
	}
	if isDigit(char) || char == '-' {
		return l.readNumber()
	}
	if char == '\'' {
		return l.readString()
	}

	switch char {
	case '*':
		l.advance()
		return Token{Type: ASTERISK, Value: "*", Line: l.Line, Col: l.column - 1}
	case ',':
		l.advance()
		return Token{Type: COMMA, Value: ",", Line: l.Line, Col: l.column - 1}
	case ';':
		l.advance()
		return Token{Type: SEMICOLON, Value: ";", Line: l.Line, Col: l.column - 1}
	case '=':
		l.advance()
		return Token{Type: EQ, Value: "=", Line: l.Line, Col: l.column - 1}
	case '!':
		l.advance()
		if l.peek() == '=' {
			l.advance()
			return Token{Type: NOT_EQ, Value: "!=", Line: l.Line, Col: l.column - 2}
		}
		return Token{Type: ILLEGAL, Value: "!", Line: l.Line, Col: l.column - 1}
	case '>':
		l.advance()
		if l.peek() == '=' {
			l.advance()
			return Token{Type: GTE, Value: ">=", Line: l.Line, Col: l.column - 2}
		}
		return Token{Type: GT, Value: ">", Line: l.Line, Col: l.column - 1}
	case '<':
		l.advance()
		if l.peek() == '=' {
			l.advance()
			return Token{Type: LTE, Value: "<=", Line: l.Line, Col: l.column - 2}
		}
		return Token{Type: LT, Value: "<", Line: l.Line, Col: l.column - 1}
	case '(':
		l.advance()
		return Token{Type: LPAREN, Value: "(", Line: l.Line, Col: l.column - 1}
	case ')':
		l.advance()
		return Token{Type: RPAREN, Value: ")", Line: l.Line, Col: l.column - 1}
	}

	l.advance()
	return Token{Type: ILLEGAL, Value: string(char), Line: l.Line, Col: l.column - 1}
}

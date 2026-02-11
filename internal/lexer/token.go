package lexer

type TokenType string

const (
	SELECT_TOKEN TokenType = "SELECT"
	FROM_TOKEN   TokenType = "FROM"
	TRUE_TOKEN   TokenType = "TRUE"
	FALSE_TOKEN  TokenType = "FALSE"
	EOF_TOKEN    TokenType = "EOF"
	IDENTIFIER   TokenType = "IDENTIFIER"
	NUMBER       TokenType = "NUMBER"
	COMMA        TokenType = ","
	SEMICOLON    TokenType = ";"
	ILLEGAL      TokenType = "ILLEGAL"
)

type Token struct {
	Type  TokenType
	Value string
	line  int
	col   int
}

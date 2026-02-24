package lexer

type TokenType string

const (
	SELECT_TOKEN TokenType = "SELECT"
	INSERT_TOKEN TokenType = "INSERT"
	UPDATE_TOKEN TokenType = "UPDATE"
	DELETE_TOKEN TokenType = "DELETE"
	WHERE_TOKEN  TokenType = "WHERE"
	FROM_TOKEN   TokenType = "FROM"
	VALUES_TOKEN TokenType = "VALUES"
	INTO_TOKEN   TokenType = "INTO"
	SET_TOKEN    TokenType = "SET"
	TRUE_TOKEN   TokenType = "TRUE"
	FALSE_TOKEN  TokenType = "FALSE"
	EOF_TOKEN    TokenType = "EOF"
	IDENTIFIER   TokenType = "IDENTIFIER"
	NUMBER       TokenType = "NUMBER"
	COMMA        TokenType = ","
	SEMICOLON    TokenType = ";"
	ILLEGAL      TokenType = "ILLEGAL"
	ASTERISK     TokenType = "*"
	EQ           TokenType = "="
	NOT_EQ       TokenType = "!="
	GT           TokenType = ">"
	LT           TokenType = "<"
	GTE          TokenType = ">="
	LTE          TokenType = "<="
	LPAREN       TokenType = "("
	RPAREN       TokenType = ")"
)

type Token struct {
	Type  TokenType
	Value string
	Line  int
	Col   int
}

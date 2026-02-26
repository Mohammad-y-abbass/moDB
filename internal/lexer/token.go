package lexer

type TokenType string

const (
	SELECT_TOKEN     TokenType = "SELECT"
	INSERT_TOKEN     TokenType = "INSERT"
	UPDATE_TOKEN     TokenType = "UPDATE"
	DELETE_TOKEN     TokenType = "DELETE"
	WHERE_TOKEN      TokenType = "WHERE"
	FROM_TOKEN       TokenType = "FROM"
	VALUES_TOKEN     TokenType = "VALUES"
	INTO_TOKEN       TokenType = "INTO"
	SET_TOKEN        TokenType = "SET"
	CREATE_TOKEN     TokenType = "CREATE"
	DATABASE_TOKEN   TokenType = "DATABASE"
	USE_TOKEN        TokenType = "USE"
	TABLE_TOKEN      TokenType = "TABLE"
	INT_TOKEN        TokenType = "INT"
	TEXT_TOKEN       TokenType = "TEXT"
	NOT_TOKEN        TokenType = "NOT"
	NULL_TOKEN       TokenType = "NULL"
	UNIQUE_TOKEN     TokenType = "UNIQUE"
	PRIMARY_TOKEN    TokenType = "PRIMARY"
	KEY_TOKEN        TokenType = "KEY"
	TRUE_TOKEN       TokenType = "TRUE"
	FALSE_TOKEN      TokenType = "FALSE"
	JOIN_TOKEN       TokenType = "JOIN"
	ON_TOKEN         TokenType = "ON"
	REFERENCES_TOKEN TokenType = "REFERENCES"
	FOREIGN_TOKEN    TokenType = "FOREIGN"
	EOF_TOKEN        TokenType = "EOF"
	IDENTIFIER       TokenType = "IDENTIFIER"
	NUMBER           TokenType = "NUMBER"
	COMMA            TokenType = ","
	SEMICOLON        TokenType = ";"
	ILLEGAL          TokenType = "ILLEGAL"
	ASTERISK         TokenType = "*"
	EQ               TokenType = "="
	NOT_EQ           TokenType = "!="
	GT               TokenType = ">"
	LT               TokenType = "<"
	GTE              TokenType = ">="
	LTE              TokenType = "<="
	LPAREN           TokenType = "("
	RPAREN           TokenType = ")"
	STRING           TokenType = "STRING"
)

type Token struct {
	Type  TokenType
	Value string
	Line  int
	Col   int
}

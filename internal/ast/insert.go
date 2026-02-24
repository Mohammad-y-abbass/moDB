package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type InsertStatement struct {
	Token   lexer.Token
	Table   string
	Columns []string
	Values  []string
}

func (is *InsertStatement) StatementNode() {}

func (is *InsertStatement) TokenLiteral() string {
	return is.Token.Value
}

func (is *InsertStatement) String() string {
	return "INSERT statement"
}

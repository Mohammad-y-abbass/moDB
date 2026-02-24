package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type SelectStatement struct {
	Token   lexer.Token
	Columns []string
	Table   string
	Where   *WhereClause
}

func (ss *SelectStatement) StatementNode() {}

func (ss *SelectStatement) TokenLiteral() string {
	return ss.Token.Value
}

func (ss *SelectStatement) String() string {
	return "SELECT statement"
}

package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type UpdateStatement struct {
	Token lexer.Token
	Table string
	Sets  map[string]string
	Where *WhereClause
}

func (us *UpdateStatement) StatementNode() {}

func (us *UpdateStatement) TokenLiteral() string {
	return us.Token.Value
}

func (us *UpdateStatement) String() string {
	return "UPDATE statement"
}

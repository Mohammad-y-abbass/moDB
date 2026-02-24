package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type DeleteStatement struct {
	Token lexer.Token
	Table string
	Where *WhereClause
}

func (ds *DeleteStatement) StatementNode() {}

func (ds *DeleteStatement) TokenLiteral() string {
	return ds.Token.Value
}

func (ds *DeleteStatement) String() string {
	return "DELETE statement"
}

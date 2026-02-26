package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

// JoinClause holds the right-side table and the ON equality columns for an INNER JOIN.
type JoinClause struct {
	Table    string // right-side table name
	LeftKey  string // qualified column reference on the left, e.g. "orders.user_id"
	RightKey string // qualified column reference on the right, e.g. "users.id"
}

type SelectStatement struct {
	Token   lexer.Token
	Columns []string
	Table   string
	Join    *JoinClause // nil for plain SELECT
	Where   *WhereClause
}

func (ss *SelectStatement) StatementNode() {}

func (ss *SelectStatement) TokenLiteral() string {
	return ss.Token.Value
}

func (ss *SelectStatement) String() string {
	return "SELECT statement"
}

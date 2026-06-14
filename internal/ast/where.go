package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type WhereClause struct {
	Token  lexer.Token // the 'WHERE' token
	Left   string      // column name
	Op     string      // '=', '!=', '>', '<', '>=', '<=', 'LIKE', 'IS NULL', 'BETWEEN', 'IN'
	Right  string      // value (or lower bound for BETWEEN)
	Right2 string      // upper bound for BETWEEN
	InList []string    // value list for IN
}

func (wc *WhereClause) Node()                {}
func (wc *WhereClause) TokenLiteral() string { return wc.Token.Value }
func (wc *WhereClause) String() string       { return "WHERE " + wc.Left + " " + wc.Op + " " + wc.Right }

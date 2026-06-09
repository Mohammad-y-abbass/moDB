package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type ShowDatabasesStatement struct {
	Token lexer.Token
}

func (s *ShowDatabasesStatement) StatementNode() {}

func (s *ShowDatabasesStatement) TokenLiteral() string {
	return s.Token.Value
}

func (s *ShowDatabasesStatement) String() string {
	return "SHOW DATABASES"
}

type ShowTablesStatement struct {
	Token lexer.Token
}

func (s *ShowTablesStatement) StatementNode() {}

func (s *ShowTablesStatement) TokenLiteral() string {
	return s.Token.Value
}

func (s *ShowTablesStatement) String() string {
	return "SHOW TABLES"
}

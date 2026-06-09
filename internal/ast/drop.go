package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type DropTableStatement struct {
	Token lexer.Token
	Table string
}

func (ds *DropTableStatement) StatementNode() {}

func (ds *DropTableStatement) TokenLiteral() string {
	return ds.Token.Value
}

func (ds *DropTableStatement) String() string {
	return "DROP TABLE " + ds.Table
}

type DropDatabaseStatement struct {
	Token        lexer.Token
	DatabaseName string
}

func (dds *DropDatabaseStatement) StatementNode() {}

func (dds *DropDatabaseStatement) TokenLiteral() string {
	return dds.Token.Value
}

func (dds *DropDatabaseStatement) String() string {
	return "DROP DATABASE " + dds.DatabaseName
}

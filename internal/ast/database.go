package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

type CreateDatabaseStatement struct {
	Token        lexer.Token // the 'CREATE' token
	DatabaseName string
}

func (cs *CreateDatabaseStatement) StatementNode() {}

func (cs *CreateDatabaseStatement) TokenLiteral() string {
	return cs.Token.Value
}

func (cs *CreateDatabaseStatement) String() string {
	return "CREATE DATABASE " + cs.DatabaseName
}

type UseDatabaseStatement struct {
	Token        lexer.Token // the 'USE' token
	DatabaseName string
}

func (us *UseDatabaseStatement) StatementNode() {}

func (us *UseDatabaseStatement) TokenLiteral() string {
	return us.Token.Value
}

func (us *UseDatabaseStatement) String() string {
	return "USE " + us.DatabaseName
}

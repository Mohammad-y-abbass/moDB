package ast

import "github.com/Mohammad-y-abbass/moDB/internal/lexer"

// ForeignKeyRef describes a REFERENCES parent_table(parent_col) constraint.
type ForeignKeyRef struct {
	Table  string
	Column string
}

type ColumnDefinition struct {
	Name         string
	DataType     string
	Size         int // e.g., 255 for TEXT(255)
	IsNullable   bool
	IsUnique     bool
	IsPrimaryKey bool
	References   *ForeignKeyRef // nil if not a FK
}

type CreateTableStatement struct {
	Token   lexer.Token // the 'CREATE' token
	Table   string
	Columns []ColumnDefinition
}

func (cs *CreateTableStatement) StatementNode() {}

func (cs *CreateTableStatement) TokenLiteral() string {
	return cs.Token.Value
}

func (cs *CreateTableStatement) String() string {
	return "CREATE TABLE statement"
}

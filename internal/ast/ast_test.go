package ast

import (
	"testing"

	"github.com/Mohammad-y-abbass/moDB/internal/lexer"
)

func TestProgramTokenLiteral(t *testing.T) {
	program := &Program{Statements: []Statement{
		&SelectStatement{Token: lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"}},
	}}
	if program.TokenLiteral() != "SELECT" {
		t.Errorf("expected SELECT, got %s", program.TokenLiteral())
	}

	empty := &Program{}
	if empty.TokenLiteral() != "" {
		t.Errorf("expected empty string, got %s", empty.TokenLiteral())
	}
}

func TestProgramString(t *testing.T) {
	program := &Program{Statements: []Statement{
		&SelectStatement{Token: lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"}},
		&InsertStatement{Token: lexer.Token{Type: lexer.INSERT_TOKEN, Value: "INSERT"}},
	}}
	out := program.String()
	if out != "SELECT statement\nINSERT statement\n" {
		t.Errorf("unexpected output: %q", out)
	}
}

func TestStatementTokenLiteral(t *testing.T) {
	tok := lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT", Line: 1, Col: 1}
	sel := &SelectStatement{Token: tok}
	if sel.TokenLiteral() != "SELECT" {
		t.Errorf("expected SELECT, got %s", sel.TokenLiteral())
	}
	sel.StatementNode()
	if sel.String() != "SELECT statement" {
		t.Errorf("unexpected String(): %q", sel.String())
	}

	ins := &InsertStatement{Token: lexer.Token{Type: lexer.INSERT_TOKEN, Value: "INSERT"}}
	if ins.TokenLiteral() != "INSERT" {
		t.Errorf("expected INSERT, got %s", ins.TokenLiteral())
	}
	if ins.String() != "INSERT statement" {
		t.Errorf("unexpected String(): %q", ins.String())
	}

	upd := &UpdateStatement{Token: lexer.Token{Type: lexer.UPDATE_TOKEN, Value: "UPDATE"}}
	if upd.TokenLiteral() != "UPDATE" {
		t.Errorf("expected UPDATE, got %s", upd.TokenLiteral())
	}
	if upd.String() != "UPDATE statement" {
		t.Errorf("unexpected String(): %q", upd.String())
	}

	del := &DeleteStatement{Token: lexer.Token{Type: lexer.DELETE_TOKEN, Value: "DELETE"}}
	if del.TokenLiteral() != "DELETE" {
		t.Errorf("expected DELETE, got %s", del.TokenLiteral())
	}
	if del.String() != "DELETE statement" {
		t.Errorf("unexpected String(): %q", del.String())
	}

	createDB := &CreateDatabaseStatement{Token: lexer.Token{Type: lexer.CREATE_TOKEN, Value: "CREATE"}, DatabaseName: "mydb"}
	if createDB.TokenLiteral() != "CREATE" {
		t.Errorf("expected CREATE, got %s", createDB.TokenLiteral())
	}
	if createDB.String() != "CREATE DATABASE mydb" {
		t.Errorf("unexpected String(): %q", createDB.String())
	}

	useDB := &UseDatabaseStatement{Token: lexer.Token{Type: lexer.USE_TOKEN, Value: "USE"}, DatabaseName: "mydb"}
	if useDB.TokenLiteral() != "USE" {
		t.Errorf("expected USE, got %s", useDB.TokenLiteral())
	}
	if useDB.String() != "USE mydb" {
		t.Errorf("unexpected String(): %q", useDB.String())
	}

	createTbl := &CreateTableStatement{Token: lexer.Token{Type: lexer.CREATE_TOKEN, Value: "CREATE"}, Table: "users"}
	if createTbl.TokenLiteral() != "CREATE" {
		t.Errorf("expected CREATE, got %s", createTbl.TokenLiteral())
	}
	if createTbl.String() != "CREATE TABLE statement" {
		t.Errorf("unexpected String(): %q", createTbl.String())
	}
}

func TestWhereClause(t *testing.T) {
	w := &WhereClause{
		Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
		Left:  "id",
		Op:    "=",
		Right: "1",
	}
	if w.TokenLiteral() != "WHERE" {
		t.Errorf("expected WHERE, got %s", w.TokenLiteral())
	}
	if w.String() != "WHERE id = 1" {
		t.Errorf("unexpected String(): %q", w.String())
	}
	w.Node()
}

func TestIdentifier(t *testing.T) {
	i := &Identifier{
		Token: lexer.Token{Type: lexer.IDENTIFIER, Value: "users"},
		Value: "users",
	}
	if i.TokenLiteral() != "users" {
		t.Errorf("expected users, got %s", i.TokenLiteral())
	}
	if i.String() != "users" {
		t.Errorf("unexpected String(): %q", i.String())
	}
	i.expressionNode()
}

func TestColumnDefinition(t *testing.T) {
	col := ColumnDefinition{
		Name:         "id",
		DataType:     "INT",
		Size:         0,
		IsNullable:   false,
		IsUnique:     true,
		IsPrimaryKey: true,
		References:   &ForeignKeyRef{Table: "other", Column: "other_id"},
	}
	if col.Name != "id" || col.DataType != "INT" || !col.IsUnique || !col.IsPrimaryKey || !col.IsNullable == false {
		t.Errorf("ColumnDefinition fields mismatch")
	}
	if col.References.Table != "other" || col.References.Column != "other_id" {
		t.Errorf("ForeignKeyRef fields mismatch")
	}
}

func TestJoinClause(t *testing.T) {
	j := &JoinClause{
		Table:    "users",
		LeftKey:  "orders.user_id",
		RightKey: "users.id",
	}
	if j.Table != "users" || j.LeftKey != "orders.user_id" || j.RightKey != "users.id" {
		t.Errorf("JoinClause fields mismatch")
	}
}

func TestSortExpression(t *testing.T) {
	se := SortExpression{Column: "name", Direction: "DESC"}
	if se.Column != "name" || se.Direction != "DESC" {
		t.Errorf("SortExpression fields mismatch")
	}
}

func TestSelectStatementWithOrderBy(t *testing.T) {
	sel := &SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "users",
		OrderBy: []SortExpression{
			{Column: "name", Direction: "ASC"},
			{Column: "age", Direction: "DESC"},
		},
	}
	if len(sel.OrderBy) != 2 {
		t.Errorf("expected 2 sort expressions, got %d", len(sel.OrderBy))
	}
	if sel.OrderBy[0].Column != "name" || sel.OrderBy[0].Direction != "ASC" {
		t.Errorf("first sort expr mismatch")
	}
	if sel.OrderBy[1].Column != "age" || sel.OrderBy[1].Direction != "DESC" {
		t.Errorf("second sort expr mismatch")
	}
}

func TestSelectStatementWithLimitOffset(t *testing.T) {
	sel := &SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "users",
		Limit:   10,
		Offset:  5,
	}
	if sel.Limit != 10 {
		t.Errorf("expected limit 10, got %d", sel.Limit)
	}
	if sel.Offset != 5 {
		t.Errorf("expected offset 5, got %d", sel.Offset)
	}
}

func TestSelectStatementWithJoin(t *testing.T) {
	sel := &SelectStatement{
		Token:   lexer.Token{Type: lexer.SELECT_TOKEN, Value: "SELECT"},
		Columns: []string{"*"},
		Table:   "orders",
		Join: &JoinClause{
			Table:    "users",
			LeftKey:  "orders.user_id",
			RightKey: "users.id",
		},
		Where: &WhereClause{
			Token: lexer.Token{Type: lexer.WHERE_TOKEN, Value: "WHERE"},
			Left:  "users.name",
			Op:    "=",
			Right: "john",
		},
	}
	if sel.Table != "orders" || sel.Join.Table != "users" {
		t.Errorf("Select with join fields mismatch")
	}
}

func TestInsertStatementWithoutColumns(t *testing.T) {
	ins := &InsertStatement{
		Token:   lexer.Token{Type: lexer.INSERT_TOKEN, Value: "INSERT"},
		Table:   "users",
		Columns: nil,
		Values:  []string{"1", "john"},
	}
	if len(ins.Columns) != 0 || len(ins.Values) != 2 {
		t.Errorf("Insert without columns fields mismatch")
	}
}

func TestUpdateStatement(t *testing.T) {
	upd := &UpdateStatement{
		Token: lexer.Token{Type: lexer.UPDATE_TOKEN, Value: "UPDATE"},
		Table: "users",
		Sets:  map[string]string{"name": "john", "age": "30"},
		Where: nil,
	}
	if len(upd.Sets) != 2 || upd.Sets["name"] != "john" {
		t.Errorf("Update statement fields mismatch")
	}
}

func TestShowDatabasesStatement(t *testing.T) {
	stmt := &ShowDatabasesStatement{
		Token: lexer.Token{Type: lexer.SHOW_TOKEN, Value: "SHOW"},
	}
	if stmt.TokenLiteral() != "SHOW" {
		t.Errorf("expected SHOW, got %s", stmt.TokenLiteral())
	}
	if stmt.String() != "SHOW DATABASES" {
		t.Errorf("unexpected String(): %q", stmt.String())
	}
}

func TestShowTablesStatement(t *testing.T) {
	stmt := &ShowTablesStatement{
		Token: lexer.Token{Type: lexer.SHOW_TOKEN, Value: "SHOW"},
	}
	if stmt.TokenLiteral() != "SHOW" {
		t.Errorf("expected SHOW, got %s", stmt.TokenLiteral())
	}
	if stmt.String() != "SHOW TABLES" {
		t.Errorf("unexpected String(): %q", stmt.String())
	}
}

func TestDropTableStatement(t *testing.T) {
	stmt := &DropTableStatement{
		Token: lexer.Token{Type: lexer.DROP_TOKEN, Value: "DROP"},
		Table: "users",
	}
	if stmt.TokenLiteral() != "DROP" {
		t.Errorf("expected DROP, got %s", stmt.TokenLiteral())
	}
	if stmt.String() != "DROP TABLE users" {
		t.Errorf("unexpected String(): %q", stmt.String())
	}
}

func TestDropDatabaseStatement(t *testing.T) {
	stmt := &DropDatabaseStatement{
		Token:        lexer.Token{Type: lexer.DROP_TOKEN, Value: "DROP"},
		DatabaseName: "mydb",
	}
	if stmt.TokenLiteral() != "DROP" {
		t.Errorf("expected DROP, got %s", stmt.TokenLiteral())
	}
	if stmt.String() != "DROP DATABASE mydb" {
		t.Errorf("unexpected String(): %q", stmt.String())
	}
}

func TestDeleteStatement(t *testing.T) {
	del := &DeleteStatement{
		Token: lexer.Token{Type: lexer.DELETE_TOKEN, Value: "DELETE"},
		Table: "users",
		Where: nil,
	}
	if del.Table != "users" {
		t.Errorf("Delete statement table mismatch")
	}
}

func TestCreateTableStatement(t *testing.T) {
	ct := &CreateTableStatement{
		Token: lexer.Token{Type: lexer.CREATE_TOKEN, Value: "CREATE"},
		Table: "users",
		Columns: []ColumnDefinition{
			{Name: "id", DataType: "INT", IsNullable: false, IsPrimaryKey: true},
			{Name: "name", DataType: "TEXT", Size: 100, IsNullable: true},
		},
	}
	if len(ct.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(ct.Columns))
	}
}

package parser

import (
	"testing"

	"github.com/Mohammad-y-abbass/moDB/internal/ast"
	"github.com/Mohammad-y-abbass/moDB/internal/lexer"
)

func TestParseSelectStatement(t *testing.T) {
	tests := []struct {
		input           string
		expectedColumns []string
		expectedTable   string
	}{
		{
			input:           "SELECT * FROM users",
			expectedColumns: []string{"*"},
			expectedTable:   "users",
		},
		{
			input:           "SELECT name FROM users",
			expectedColumns: []string{"name"},
			expectedTable:   "users",
		},
		{
			input:           "SELECT name, age, email FROM customers",
			expectedColumns: []string{"name", "age", "email"},
			expectedTable:   "customers",
		},
		{
			input:           "select * from users",
			expectedColumns: []string{"*"},
			expectedTable:   "users",
		},
	}

	for _, tt := range tests {
		l := lexer.New(tt.input)
		p := New(l)
		program := p.ParseProgram()
		checkParserErrors(t, p)

		if len(program.Statements) != 1 {
			t.Fatalf("input %q: expected 1 statement, got %d", tt.input, len(program.Statements))
		}

		stmt, ok := program.Statements[0].(*ast.SelectStatement)
		if !ok {
			t.Fatalf("input %q: expected *ast.SelectStatement, got %T", tt.input, program.Statements[0])
		}

		if len(stmt.Columns) != len(tt.expectedColumns) {
			t.Errorf("input %q: expected %d columns, got %d", tt.input, len(tt.expectedColumns), len(stmt.Columns))
		}

		for i, col := range tt.expectedColumns {
			if stmt.Columns[i] != col {
				t.Errorf("input %q: expected column %d to be %q, got %q", tt.input, i, col, stmt.Columns[i])
			}
		}

		if stmt.Table != tt.expectedTable {
			t.Errorf("input %q: expected table %q, got %q", tt.input, tt.expectedTable, stmt.Table)
		}
	}
}

func TestParserErrors(t *testing.T) {
	tests := []struct {
		input         string
		expectedError string
	}{
		{
			input:         "SELECT",
			expectedError: "Expected column name or '*' after SELECT at line 1, column 7, but got ''",
		},
		{
			input:         "SELECT *",
			expectedError: "Expected FROM keyword at line 1, column 9, but got ''",
		},
		{
			input:         "SELECT * FROM",
			expectedError: "Expected table name after FROM at line 1, column 14, but got ''",
		},
		{
			input:         "SELECT name, FROM users",
			expectedError: "Expected column name after comma at line 1, column 14, but got 'FROM'",
		},
		{
			input:         "FROM users",
			expectedError: "Unexpected token 'FROM' at line 1, column 1. Expected a statement (e.g., SELECT)",
		},
	}

	for _, tt := range tests {
		l := lexer.New(tt.input)
		p := New(l)
		p.ParseProgram()

		errors := p.Errors()
		if len(errors) == 0 {
			t.Errorf("input %q: expected parser error, got none", tt.input)
			continue
		}

		if errors[0] != tt.expectedError {
			t.Errorf("input %q: expected error %q, got %q", tt.input, tt.expectedError, errors[0])
		}
	}
}

func TestParseWhereClause(t *testing.T) {
	input := "SELECT * FROM users WHERE id = 1"
	l := lexer.New(input)
	p := New(l)
	program := p.ParseProgram()
	checkParserErrors(t, p)

	stmt := program.Statements[0].(*ast.SelectStatement)
	if stmt.Where == nil {
		t.Fatal("Where clause is nil")
	}
	if stmt.Where.Left != "id" {
		t.Errorf("expected id, got %s", stmt.Where.Left)
	}
	if stmt.Where.Op != "=" {
		t.Errorf("expected =, got %s", stmt.Where.Op)
	}
	if stmt.Where.Right != "1" {
		t.Errorf("expected 1, got %s", stmt.Where.Right)
	}
}

func TestParseInsertStatement(t *testing.T) {
	input := "INSERT INTO users (name, age) VALUES (john, 30)"
	l := lexer.New(input)
	p := New(l)
	program := p.ParseProgram()
	checkParserErrors(t, p)

	stmt := program.Statements[0].(*ast.InsertStatement)
	if stmt.Table != "users" {
		t.Errorf("expected users, got %s", stmt.Table)
	}
	if len(stmt.Columns) != 2 || stmt.Columns[0] != "name" || stmt.Columns[1] != "age" {
		t.Errorf("columns mismatch: %v", stmt.Columns)
	}
	if len(stmt.Values) != 2 || stmt.Values[0] != "john" || stmt.Values[1] != "30" {
		t.Errorf("values mismatch: %v", stmt.Values)
	}
}

func TestParseUpdateStatement(t *testing.T) {
	input := "UPDATE users SET age = 31, name = johnny WHERE id = 1"
	l := lexer.New(input)
	p := New(l)
	program := p.ParseProgram()
	checkParserErrors(t, p)

	stmt := program.Statements[0].(*ast.UpdateStatement)
	if stmt.Table != "users" {
		t.Errorf("expected users, got %s", stmt.Table)
	}
	if stmt.Sets["age"] != "31" || stmt.Sets["name"] != "johnny" {
		t.Errorf("sets mismatch: %v", stmt.Sets)
	}
	if stmt.Where == nil || stmt.Where.Left != "id" {
		t.Errorf("where mismatch")
	}
}

func TestParseDeleteStatement(t *testing.T) {
	input := "DELETE FROM users WHERE id = 1"
	l := lexer.New(input)
	p := New(l)
	program := p.ParseProgram()
	checkParserErrors(t, p)

	stmt := program.Statements[0].(*ast.DeleteStatement)
	if stmt.Table != "users" {
		t.Errorf("expected users, got %s", stmt.Table)
	}
	if stmt.Where == nil || stmt.Where.Left != "id" {
		t.Errorf("where mismatch")
	}
}

func checkParserErrors(t *testing.T, p *Parser) {
	errors := p.Errors()
	if len(errors) == 0 {
		return
	}

	t.Errorf("parser has %d errors", len(errors))
	for _, msg := range errors {
		t.Errorf("parser error: %q", msg)
	}
	t.FailNow()
}

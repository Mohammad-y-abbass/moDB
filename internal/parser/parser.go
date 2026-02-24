package parser

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Mohammad-y-abbass/moDB/internal/ast"
	"github.com/Mohammad-y-abbass/moDB/internal/lexer"
)

type Parser struct {
	l            *lexer.Lexer
	currentToken lexer.Token
	peekToken    lexer.Token
	errors       []string
}

func New(l *lexer.Lexer) *Parser {
	p := &Parser{
		l:      l,
		errors: []string{},
	}
	// Read two tokens to fill currentToken and peekToken
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) Errors() []string {
	return p.errors
}

func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, msg)
}

func (p *Parser) ParseProgram() *ast.Program {
	program := &ast.Program{Statements: []ast.Statement{}}

	for p.currentToken.Type != lexer.EOF_TOKEN {
		stmt := p.parseStatement()
		if stmt != nil {
			program.Statements = append(program.Statements, stmt)
		}
		p.nextToken()
	}
	return program
}

func (p *Parser) parseStatement() ast.Statement {
	switch p.currentToken.Type {
	case lexer.SELECT_TOKEN:
		return p.parseSelectStatement()
	case lexer.INSERT_TOKEN:
		return p.parseInsertStatement()
	case lexer.UPDATE_TOKEN:
		return p.parseUpdateStatement()
	case lexer.DELETE_TOKEN:
		return p.parseDeleteStatement()
	case lexer.CREATE_TOKEN:
		return p.parseCreateStatement()
	case lexer.USE_TOKEN:
		return p.parseUseStatement()
	case lexer.ILLEGAL:
		p.addError(fmt.Sprintf("Illegal character '%s' at line %d, column %d",
			p.currentToken.Value, p.currentToken.Line, p.currentToken.Col))
		return nil
	case lexer.SEMICOLON:
		return nil
	case lexer.EOF_TOKEN:
		return nil
	default:
		p.addError(fmt.Sprintf("Unexpected token '%s' at line %d, column %d. Expected a statement (e.g., SELECT)",
			p.currentToken.Value, p.currentToken.Line, p.currentToken.Col))
		return nil
	}
}

func (p *Parser) parseSelectStatement() *ast.SelectStatement {
	stmt := &ast.SelectStatement{Token: p.currentToken}

	p.nextToken()

	// Check for columns or asterisk
	switch p.currentToken.Type {
	case lexer.ASTERISK:
		stmt.Columns = []string{"*"}
		p.nextToken()
	case lexer.IDENTIFIER:
		stmt.Columns = p.parseColumns()
	default:
		p.addError(fmt.Sprintf("Expected column name or '*' after SELECT at line %d, column %d, but got '%s'",
			p.currentToken.Line, p.currentToken.Col, p.currentToken.Value))
		return nil
	}

	// Expect FROM keyword
	if p.currentToken.Type != lexer.FROM_TOKEN {
		p.addError(fmt.Sprintf("Expected FROM keyword at line %d, column %d, but got '%s'",
			p.currentToken.Line, p.currentToken.Col, p.currentToken.Value))
		return nil
	}

	p.nextToken()

	// Expect table name
	if p.currentToken.Type != lexer.IDENTIFIER {
		p.addError(fmt.Sprintf("Expected table name after FROM at line %d, column %d, but got '%s'",
			p.currentToken.Line, p.currentToken.Col, p.currentToken.Value))
		return nil
	}

	stmt.Table = p.currentToken.Value

	if p.peekToken.Type == lexer.WHERE_TOKEN {
		p.nextToken() // move to WHERE
		p.nextToken() // move to identifier
		stmt.Where = p.parseWhereClause()
	}

	return stmt
}

func (p *Parser) parseInsertStatement() *ast.InsertStatement {
	stmt := &ast.InsertStatement{Token: p.currentToken}

	if p.peekToken.Type != lexer.INTO_TOKEN {
		p.addError("Expected INTO after INSERT")
		return nil
	}
	p.nextToken() // Move to INTO

	if p.peekToken.Type != lexer.IDENTIFIER {
		p.addError("Expected table name after INTO")
		return nil
	}
	p.nextToken() // Move to table name
	stmt.Table = p.currentToken.Value

	if p.peekToken.Type == lexer.LPAREN {
		p.nextToken() // Move to (
		p.nextToken() // Move to first col
		stmt.Columns = p.parseCommaSeparatedList(lexer.RPAREN)
	}

	if p.peekToken.Type != lexer.VALUES_TOKEN {
		p.addError("Expected VALUES keyword")
		return nil
	}
	p.nextToken() // Move to VALUES

	if p.peekToken.Type != lexer.LPAREN {
		p.addError("Expected ( after VALUES")
		return nil
	}
	p.nextToken() // Move to (
	p.nextToken() // Move to first val
	stmt.Values = p.parseCommaSeparatedList(lexer.RPAREN)

	return stmt
}

func (p *Parser) parseUpdateStatement() *ast.UpdateStatement {
	stmt := &ast.UpdateStatement{Token: p.currentToken}

	if p.peekToken.Type != lexer.IDENTIFIER {
		p.addError("Expected table name after UPDATE")
		return nil
	}
	p.nextToken()
	stmt.Table = p.currentToken.Value

	if p.peekToken.Type != lexer.SET_TOKEN {
		p.addError("Expected SET keyword")
		return nil
	}
	p.nextToken()

	stmt.Sets = make(map[string]string)
	for {
		p.nextToken() // Move to col
		if p.currentToken.Type != lexer.IDENTIFIER {
			p.addError("Expected column name in SET")
			return nil
		}
		col := p.currentToken.Value

		if p.peekToken.Type != lexer.EQ {
			p.addError("Expected = in SET")
			return nil
		}
		p.nextToken()
		p.nextToken() // Move to val

		if p.currentToken.Type != lexer.IDENTIFIER && p.currentToken.Type != lexer.NUMBER && p.currentToken.Type != lexer.STRING {
			p.addError("Expected value in SET")
			return nil
		}
		stmt.Sets[col] = p.currentToken.Value

		if p.peekToken.Type == lexer.COMMA {
			p.nextToken() // Move to comma
		} else {
			break
		}
	}

	if p.peekToken.Type == lexer.WHERE_TOKEN {
		p.nextToken()
		p.nextToken()
		stmt.Where = p.parseWhereClause()
	}

	return stmt
}

func (p *Parser) parseDeleteStatement() *ast.DeleteStatement {
	stmt := &ast.DeleteStatement{Token: p.currentToken}

	if p.peekToken.Type != lexer.FROM_TOKEN {
		p.addError("Expected FROM after DELETE")
		return nil
	}
	p.nextToken()

	if p.peekToken.Type != lexer.IDENTIFIER {
		p.addError("Expected table name after FROM")
		return nil
	}
	p.nextToken()
	stmt.Table = p.currentToken.Value

	if p.peekToken.Type == lexer.WHERE_TOKEN {
		p.nextToken()
		p.nextToken()
		stmt.Where = p.parseWhereClause()
	}

	return stmt
}

func (p *Parser) parseCreateStatement() ast.Statement {
	if p.peekToken.Type == lexer.DATABASE_TOKEN {
		return p.parseCreateDatabaseStatement()
	} else if p.peekToken.Type == lexer.TABLE_TOKEN {
		return p.parseCreateTableStatement()
	} else {
		p.addError(fmt.Sprintf("Expected DATABASE or TABLE after CREATE at line %d, column %d, but got '%s'",
			p.peekToken.Line, p.peekToken.Col, p.peekToken.Value))
		return nil
	}
}

func (p *Parser) parseCreateDatabaseStatement() *ast.CreateDatabaseStatement {
	stmt := &ast.CreateDatabaseStatement{Token: p.currentToken}

	p.nextToken() // Move to DATABASE
	if p.peekToken.Type != lexer.IDENTIFIER {
		p.addError(fmt.Sprintf("Expected database name after CREATE DATABASE at line %d, column %d, but got '%s'",
			p.peekToken.Line, p.peekToken.Col, p.peekToken.Value))
		return nil
	}
	p.nextToken() // Move to database name
	stmt.DatabaseName = p.currentToken.Value
	return stmt
}

func (p *Parser) parseUseStatement() *ast.UseDatabaseStatement {
	stmt := &ast.UseDatabaseStatement{Token: p.currentToken}

	if p.peekToken.Type != lexer.IDENTIFIER {
		p.addError(fmt.Sprintf("Expected database name after USE at line %d, column %d, but got '%s'",
			p.peekToken.Line, p.peekToken.Col, p.peekToken.Value))
		return nil
	}
	p.nextToken() // Move to database name
	stmt.DatabaseName = p.currentToken.Value
	return stmt
}

func (p *Parser) parseCreateTableStatement() *ast.CreateTableStatement {
	stmt := &ast.CreateTableStatement{Token: p.currentToken}

	p.nextToken() // Move to TABLE

	if p.currentToken.Type != lexer.TABLE_TOKEN {
		p.addError(fmt.Sprintf("Expected TABLE after CREATE at line %d, column %d, but got '%s'",
			p.currentToken.Line, p.currentToken.Col, p.currentToken.Value))
		return nil
	}

	if p.peekToken.Type != lexer.IDENTIFIER {
		p.addError("Expected table name after CREATE TABLE")
		return nil
	}
	p.nextToken() // Move to table name
	stmt.Table = p.currentToken.Value

	if p.peekToken.Type != lexer.LPAREN {
		p.addError("Expected ( after table name")
		return nil
	}
	p.nextToken() // Move to (

	for {
		p.nextToken() // Move to column name
		col := p.parseColumnDefinition()
		if col == (ast.ColumnDefinition{}) {
			return nil
		}
		stmt.Columns = append(stmt.Columns, col)

		if p.peekToken.Type == lexer.COMMA {
			p.nextToken() // Move to comma
		} else if p.peekToken.Type == lexer.RPAREN {
			p.nextToken() // Move to )
			break
		} else {
			p.addError(fmt.Sprintf("Expected , or ) in table definition, got %s", p.peekToken.Value))
			return nil
		}
	}

	return stmt
}

func (p *Parser) parseColumnDefinition() ast.ColumnDefinition {
	if p.currentToken.Type != lexer.IDENTIFIER {
		p.addError("Expected column name")
		return ast.ColumnDefinition{}
	}
	col := ast.ColumnDefinition{Name: p.currentToken.Value, IsNullable: true}

	if p.peekToken.Type != lexer.INT_TOKEN && p.peekToken.Type != lexer.TEXT_TOKEN {
		p.addError(fmt.Sprintf("Expected data type for column %s, got %s", col.Name, p.peekToken.Value))
		return ast.ColumnDefinition{}
	}
	p.nextToken()
	col.DataType = p.currentToken.Value

	// Handle optional (size) e.g., TEXT(255)
	if p.peekToken.Type == lexer.LPAREN {
		p.nextToken() // Move to (
		if p.peekToken.Type != lexer.NUMBER {
			p.addError("Expected number for size")
			return ast.ColumnDefinition{}
		}
		p.nextToken() // Move to number
		size, _ := strconv.Atoi(p.currentToken.Value)
		col.Size = size
		if p.peekToken.Type != lexer.RPAREN {
			p.addError("Expected ) after size")
			return ast.ColumnDefinition{}
		}
		p.nextToken() // Move to )
	}

	// Parse constraints
	for p.peekToken.Type == lexer.NOT_TOKEN || p.peekToken.Type == lexer.UNIQUE_TOKEN || p.peekToken.Type == lexer.PRIMARY_TOKEN {
		p.nextToken()
		switch p.currentToken.Type {
		case lexer.NOT_TOKEN:
			if p.peekToken.Type != lexer.NULL_TOKEN {
				p.addError("Expected NULL after NOT")
				return ast.ColumnDefinition{}
			}
			p.nextToken()
			col.IsNullable = false
		case lexer.UNIQUE_TOKEN:
			col.IsUnique = true
		case lexer.PRIMARY_TOKEN:
			if p.peekToken.Type != lexer.KEY_TOKEN {
				p.addError("Expected KEY after PRIMARY")
				return ast.ColumnDefinition{}
			}
			p.nextToken()
			col.IsPrimaryKey = true
			col.IsUnique = true
			col.IsNullable = false
		}
	}

	return col
}

func (p *Parser) parseWhereClause() *ast.WhereClause {
	where := &ast.WhereClause{Token: p.currentToken}

	if p.currentToken.Type != lexer.IDENTIFIER {
		p.addError(fmt.Sprintf("Expected column name in WHERE clause, got %s", p.currentToken.Value))
		return nil
	}
	where.Left = p.currentToken.Value

	p.nextToken()
	if !isComparisonOperator(p.currentToken.Type) {
		p.addError(fmt.Sprintf("Expected comparison operator in WHERE clause, got %s", p.currentToken.Value))
		return nil
	}
	where.Op = p.currentToken.Value

	p.nextToken()
	if p.currentToken.Type != lexer.IDENTIFIER && p.currentToken.Type != lexer.NUMBER && p.currentToken.Type != lexer.STRING {
		p.addError(fmt.Sprintf("Expected value in WHERE clause, got %s", p.currentToken.Value))
		return nil
	}
	where.Right = p.currentToken.Value

	return where
}

func (p *Parser) parseCommaSeparatedList(endToken lexer.TokenType) []string {
	var list []string

	for {
		if p.currentToken.Type == lexer.IDENTIFIER || p.currentToken.Type == lexer.NUMBER || p.currentToken.Type == lexer.STRING {
			list = append(list, p.currentToken.Value)
		} else {
			p.addError(fmt.Sprintf("Expected identifier, number, or string, got %s", p.currentToken.Value))
			return nil
		}

		if p.peekToken.Type == lexer.COMMA {
			p.nextToken() // Move to comma
			p.nextToken() // Move to next item
		} else {
			break
		}
	}

	if p.peekToken.Type != endToken {
		p.addError(fmt.Sprintf("Expected %s, got %s", endToken, p.peekToken.Value))
		return nil
	}
	p.nextToken() // Move to end token

	return list
}

func isComparisonOperator(t lexer.TokenType) bool {
	switch t {
	case lexer.EQ, lexer.NOT_EQ, lexer.GT, lexer.LT, lexer.GTE, lexer.LTE:
		return true
	default:
		return false
	}
}

func (p *Parser) parseColumns() []string {
	var columns []string

	if p.currentToken.Type == lexer.IDENTIFIER {
		columns = append(columns, p.currentToken.Value)
	}

	for p.peekToken.Type == lexer.COMMA {
		p.nextToken() // Move to comma
		p.nextToken() // Move to next identifier

		if p.currentToken.Type != lexer.IDENTIFIER {
			p.addError(fmt.Sprintf("Expected column name after comma at line %d, column %d, but got '%s'",
				p.currentToken.Line, p.currentToken.Col, p.currentToken.Value))
			break
		}

		columns = append(columns, p.currentToken.Value)
	}

	p.nextToken()
	return columns
}

// GetErrorMessage returns the first parsing error if any
func (p *Parser) GetErrorMessage() string {
	if len(p.errors) == 0 {
		return ""
	}

	return fmt.Sprintf("Parsing error: %s", p.errors[0])
}

// FormatAST returns a formatted tree representation of the AST
func (p *Parser) FormatAST(program *ast.Program) string {
	if program == nil || len(program.Statements) == 0 {
		return "Program {\n  Statements: []\n}"
	}

	var builder strings.Builder
	builder.WriteString("Program {\n")
	builder.WriteString("  Statements: [\n")

	for i, stmt := range program.Statements {
		builder.WriteString(p.formatStatement(stmt, 4))
		if i < len(program.Statements)-1 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}

	builder.WriteString("  ]\n")
	builder.WriteString("}")

	return builder.String()
}

func (p *Parser) formatStatement(stmt ast.Statement, indent int) string {
	indentStr := strings.Repeat(" ", indent)

	switch s := stmt.(type) {
	case *ast.SelectStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "SelectStatement {\n")
		builder.WriteString(indentStr + "  Token: " + s.Token.Value + ",\n")
		builder.WriteString(indentStr + "  Columns: [")

		if len(s.Columns) > 0 {
			builder.WriteString("\n")
			for i, col := range s.Columns {
				builder.WriteString(indentStr + "    \"" + col + "\"")
				if i < len(s.Columns)-1 {
					builder.WriteString(",\n")
				} else {
					builder.WriteString("\n")
				}
			}
			builder.WriteString(indentStr + "  ],\n")
		} else {
			builder.WriteString("],\n")
		}

		builder.WriteString(indentStr + "  Table: \"" + s.Table + "\"")
		if s.Where != nil {
			builder.WriteString(",\n")
			builder.WriteString(indentStr + "  Where: " + s.Where.String() + "\n")
		} else {
			builder.WriteString("\n")
		}
		builder.WriteString(indentStr + "}")

		return builder.String()
	case *ast.InsertStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "InsertStatement {\n")
		builder.WriteString(indentStr + "  Table: \"" + s.Table + "\",\n")
		builder.WriteString(indentStr + "  Columns: [" + strings.Join(s.Columns, ", ") + "],\n")
		builder.WriteString(indentStr + "  Values: [" + strings.Join(s.Values, ", ") + "]\n")
		builder.WriteString(indentStr + "}")
		return builder.String()
	case *ast.UpdateStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "UpdateStatement {\n")
		builder.WriteString(indentStr + "  Table: \"" + s.Table + "\",\n")
		builder.WriteString(indentStr + "  Sets: " + fmt.Sprint(s.Sets))
		if s.Where != nil {
			builder.WriteString(",\n" + indentStr + "  Where: " + s.Where.String() + "\n")
		} else {
			builder.WriteString("\n")
		}
		builder.WriteString(indentStr + "}")
		return builder.String()
	case *ast.DeleteStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "DeleteStatement {\n")
		builder.WriteString(indentStr + "  Table: \"" + s.Table + "\"")
		if s.Where != nil {
			builder.WriteString(",\n" + indentStr + "  Where: " + s.Where.String() + "\n")
		} else {
			builder.WriteString("\n")
		}
		builder.WriteString(indentStr + "}")
		return builder.String()
	case *ast.CreateDatabaseStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "CreateDatabaseStatement {\n")
		builder.WriteString(indentStr + "  DatabaseName: \"" + s.DatabaseName + "\"\n")
		builder.WriteString(indentStr + "}")
		return builder.String()
	case *ast.UseDatabaseStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "UseDatabaseStatement {\n")
		builder.WriteString(indentStr + "  DatabaseName: \"" + s.DatabaseName + "\"\n")
		builder.WriteString(indentStr + "}")
		return builder.String()
	case *ast.CreateTableStatement:
		var builder strings.Builder
		builder.WriteString(indentStr + "CreateTableStatement {\n")
		builder.WriteString(indentStr + "  Table: \"" + s.Table + "\",\n")
		builder.WriteString(indentStr + "  Columns: [\n")
		for i, col := range s.Columns {
			builder.WriteString(fmt.Sprintf("%s    {Name: %s, Type: %s, Nullable: %v, Unique: %v, PK: %v}",
				indentStr, col.Name, col.DataType, col.IsNullable, col.IsUnique, col.IsPrimaryKey))
			if i < len(s.Columns)-1 {
				builder.WriteString(",\n")
			} else {
				builder.WriteString("\n")
			}
		}
		builder.WriteString(indentStr + "  ]\n")
		builder.WriteString(indentStr + "}")
		return builder.String()
	default:
		return indentStr + "UnknownStatement {}"
	}
}

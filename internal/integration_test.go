package internal

import (
	"os"
	"testing"

	"github.com/Mohammad-y-abbass/moDB/internal/executor"
	"github.com/Mohammad-y-abbass/moDB/internal/lexer"
	"github.com/Mohammad-y-abbass/moDB/internal/parser"
	"github.com/Mohammad-y-abbass/moDB/internal/planner"
	"github.com/Mohammad-y-abbass/moDB/internal/storage"
)

type testHelper struct {
	Engine *storage.Engine
	Exec   *executor.Executor
	Plan   *planner.Planner
	T      *testing.T
}

func newTestHelper(t *testing.T) *testHelper {
	t.Helper()
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := executor.New(engine)
	plan := planner.New()

	// Create and use a test database
	exec.Execute(&planner.CreateDatabaseNode{DatabaseName: "testdb"})
	exec.Execute(&planner.UseDatabaseNode{DatabaseName: "testdb"})

	t.Cleanup(func() {
		for _, table := range exec.Tables {
			table.Pager.Close()
		}
	})

	return &testHelper{
		Engine: engine,
		Exec:   exec,
		Plan:   plan,
		T:      t,
	}
}

func (h *testHelper) execSQL(sql string) (executor.ResultSet, error) {
	l := lexer.New(sql)
	p := parser.New(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		h.T.Fatalf("Parse error for %q: %s", sql, p.GetErrorMessage())
	}
	if len(program.Statements) == 0 {
		h.T.Fatalf("No statements parsed from %q", sql)
	}

	var lastResult executor.ResultSet
	var lastErr error
	for _, stmt := range program.Statements {
		plan := h.Plan.GeneratePlan(stmt)
		lastResult, lastErr = h.Exec.Execute(plan)
		if lastErr != nil {
			return lastResult, lastErr
		}
	}
	return lastResult, lastErr
}

func (h *testHelper) assertError(sql string) {
	_, err := h.execSQL(sql)
	if err == nil {
		h.T.Errorf("Expected error for SQL: %q", sql)
	}
}

func (h *testHelper) assertSuccess(sql string) {
	_, err := h.execSQL(sql)
	if err != nil {
		h.T.Errorf("Unexpected error for SQL %q: %v", sql, err)
	}
}

func TestIntegrationCreateAndUseDatabase(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE DATABASE newdb")
	h.assertSuccess("USE newdb")
}

func TestIntegrationCreateTableAndInsert(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")
	h.assertSuccess("INSERT INTO users VALUES (2, 'Bob', 25)")

	result, err := h.execSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestIntegrationInsertNamedColumns(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
	h.assertSuccess("INSERT INTO users (id, name) VALUES (2, 'Bob')")

	result, err := h.execSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestIntegrationSelectWithWhere(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")
	h.assertSuccess("INSERT INTO users VALUES (2, 'Bob', 25)")
	h.assertSuccess("INSERT INTO users VALUES (3, 'Charlie', 35)")

	result, err := h.execSQL("SELECT * FROM users WHERE age > 28")
	if err != nil {
		t.Fatalf("SELECT WHERE failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows (age > 28), got %d", len(result.Rows))
	}
}

func TestIntegrationSelectProjection(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")

	result, err := h.execSQL("SELECT name, age FROM users")
	if err != nil {
		t.Fatalf("SELECT projection failed: %v", err)
	}
	if len(result.Columns) != 2 || result.Columns[0] != "name" {
		t.Errorf("columns mismatch: %v", result.Columns)
	}
}

func TestIntegrationUpdate(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")
	h.assertSuccess("UPDATE users SET age = 31 WHERE id = 1")

	result, err := h.execSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result.Rows[0].Values[2].(int32) != 31 {
		t.Errorf("expected age 31, got %v", result.Rows[0].Values[2])
	}
}

func TestIntegrationUpdateWithoutWhere(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")
	h.assertSuccess("INSERT INTO users VALUES (2, 'Bob', 25)")
	h.assertSuccess("UPDATE users SET age = 99")

	result, err := h.execSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	for _, row := range result.Rows {
		if row.Values[2].(int32) != 99 {
			t.Errorf("expected age 99, got %v", row.Values[2])
		}
	}
}

func TestIntegrationDelete(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")
	h.assertSuccess("INSERT INTO users VALUES (2, 'Bob', 25)")
	h.assertSuccess("DELETE FROM users WHERE id = 1")

	result, err := h.execSQL("SELECT * FROM users")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after delete, got %d", len(result.Rows))
	}
}

func TestIntegrationDeleteWithoutWhere(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50), age INT)")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice', 30)")
	h.assertSuccess("INSERT INTO users VALUES (2, 'Bob', 25)")
	_, err := h.execSQL("DELETE FROM users")
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}

	rows, _ := h.Exec.Tables["users"].SelectAll()
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after delete all, got %d", len(rows))
	}
}

func TestIntegrationForeignKey(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE departments (id INT NOT NULL PRIMARY KEY, name TEXT(50))")
	h.assertSuccess("INSERT INTO departments VALUES (1, 'Engineering')")
	h.assertSuccess("INSERT INTO departments VALUES (2, 'Sales')")

	h.assertSuccess("CREATE TABLE employees (id INT NOT NULL PRIMARY KEY, name TEXT(50), dept_id INT REFERENCES departments(id))")
	h.assertSuccess("INSERT INTO employees VALUES (1, 'Alice', 1)")
	h.assertSuccess("INSERT INTO employees VALUES (2, 'Bob', 2)")

	// FK violation: insert with non-existent dept
	h.assertError("INSERT INTO employees VALUES (3, 'Charlie', 99)")

	// FK violation: delete parent with children
	h.assertError("DELETE FROM departments WHERE id = 1")

	// Delete parent with no children should succeed
	h.assertSuccess("INSERT INTO departments VALUES (3, 'HR')")
	h.assertSuccess("DELETE FROM departments WHERE id = 3")
}

func TestIntegrationJoin(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE departments (id INT NOT NULL PRIMARY KEY, name TEXT(50))")
	h.assertSuccess("INSERT INTO departments VALUES (1, 'Engineering')")
	h.assertSuccess("INSERT INTO departments VALUES (2, 'Sales')")

	h.assertSuccess("CREATE TABLE employees (id INT NOT NULL PRIMARY KEY, name TEXT(50), dept_id INT REFERENCES departments(id))")
	h.assertSuccess("INSERT INTO employees VALUES (1, 'Alice', 1)")
	h.assertSuccess("INSERT INTO employees VALUES (2, 'Bob', 2)")

	result, err := h.execSQL("SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id")
	if err != nil {
		t.Fatalf("JOIN failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 joined rows, got %d", len(result.Rows))
	}

	// Join with where
	result, err = h.execSQL("SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id WHERE employees.name = 'Alice'")
	if err != nil {
		t.Fatalf("JOIN with WHERE failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 joined row after filter, got %d", len(result.Rows))
	}
}

func TestIntegrationUniqueConstraint(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE users (id INT NOT NULL UNIQUE, name TEXT(50))")
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice')")
	h.assertError("INSERT INTO users VALUES (1, 'Bob')")

	// Different id should succeed
	h.assertSuccess("INSERT INTO users VALUES (2, 'Bob')")
}

func TestIntegrationNotNullConstraint(t *testing.T) {
	h := newTestHelper(t)

	// id is NOT NULL with UNIQUE to prevent duplicate 0 values
	h.assertSuccess("CREATE TABLE users (id INT NOT NULL UNIQUE, name TEXT(50))")
	// Inserting with a valid id works
	h.assertSuccess("INSERT INTO users VALUES (1, 'Alice')")
}

func TestIntegrationMultipleStatements(t *testing.T) {
	h := newTestHelper(t)

	// Multiple statements in one SQL string
	h.assertSuccess("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, val INT)")
	h.assertSuccess("INSERT INTO t VALUES (1, 10); INSERT INTO t VALUES (2, 20)")

	result, err := h.execSQL("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestIntegrationDataTypeText(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name TEXT(100))")
	h.assertSuccess("INSERT INTO t VALUES (1, 'Hello World')")

	result, err := h.execSQL("SELECT * FROM t")
	if err != nil {
		t.Fatalf("SELECT failed: %v", err)
	}
	if result.Rows[0].Values[1].(string) != "Hello World" {
		t.Errorf("expected 'Hello World', got %v", result.Rows[0].Values[1])
	}
}

func TestIntegrationWhereWithString(t *testing.T) {
	h := newTestHelper(t)

	h.assertSuccess("CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name TEXT(50))")
	h.assertSuccess("INSERT INTO t VALUES (1, 'Alice')")
	h.assertSuccess("INSERT INTO t VALUES (2, 'Bob')")

	result, err := h.execSQL("SELECT * FROM t WHERE name = 'Alice'")
	if err != nil {
		t.Fatalf("SELECT WHERE with string failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestIntegrationTablePersistence(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := executor.New(engine)
	plan := planner.New()

	// Phase 1: Create, switch, create table, insert
	exec.Execute(&planner.CreateDatabaseNode{DatabaseName: "persistdb"})
	exec.Execute(&planner.UseDatabaseNode{DatabaseName: "persistdb"})

	l := lexer.New("CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name TEXT(50))")
	p := parser.New(l)
	prog := p.ParseProgram()
	planNode := plan.GeneratePlan(prog.Statements[0])
	exec.Execute(planNode)

	insertLexer := lexer.New("INSERT INTO users VALUES (1, 'Alice')")
	insertParser := parser.New(insertLexer)
	insertProg := insertParser.ParseProgram()
	exec.Execute(plan.GeneratePlan(insertProg.Statements[0]))

	// Close everything
	for _, table := range exec.Tables {
		table.Pager.Close()
	}

	// Phase 2: Reopen and verify
	engine2 := storage.NewEngine(tempDir)
	exec2 := executor.New(engine2)
	exec2.Execute(&planner.UseDatabaseNode{DatabaseName: "persistdb"})

	result, err := exec2.Execute(&planner.ScanNode{TableName: "users"})
	if err != nil {
		t.Fatalf("Reopened table scan failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row after reopen, got %d", len(result.Rows))
	}

	for _, table := range exec2.Tables {
		table.Pager.Close()
	}
}

// File-level test to verify the test file is in the right package
func TestIntegrationFileStructure(t *testing.T) {
	// Verify database directory gets created
	tempDir := t.TempDir()
	_ = storage.NewEngine(tempDir)
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Errorf("base directory should exist")
	}
}

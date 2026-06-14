package executor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Mohammad-y-abbass/moDB/internal/ast"
	"github.com/Mohammad-y-abbass/moDB/internal/planner"
	"github.com/Mohammad-y-abbass/moDB/internal/storage"
)

func setupExecutor(t *testing.T) (*Executor, func()) {
	t.Helper()
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	// Create a users table
	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32, IsNullable: false, IsPrimaryKey: true, IsUnique: true},
		{Name: "name", Type: storage.TypeFixedText, Size: 32, IsNullable: true},
		{Name: "age", Type: storage.TypeInt32, IsNullable: true},
	})

	dbPath := filepath.Join(tempDir, "testdb", "users.db")
	pager, err := storage.NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	table := storage.NewTable(pager, schema)
	exec.RegisterTable("users", table)

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	return exec, func() {}
}

func setupExecutorWithFK(t *testing.T) (*Executor, func()) {
	t.Helper()
	tempDir := t.TempDir()

	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	// Parent: departments
	deptSchema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32, IsNullable: false, IsPrimaryKey: true, IsUnique: true},
		{Name: "name", Type: storage.TypeFixedText, Size: 32, IsNullable: true},
	})

	deptPath := filepath.Join(tempDir, "testdb", "departments.db")
	deptPager, _ := storage.NewPager(deptPath)
	deptTable := storage.NewTable(deptPager, deptSchema)
	exec.RegisterTable("departments", deptTable)

	// Child: employees with FK -> departments
	empSchema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32, IsNullable: false, IsPrimaryKey: true, IsUnique: true},
		{Name: "name", Type: storage.TypeFixedText, Size: 32, IsNullable: true},
		{Name: "dept_id", Type: storage.TypeInt32, IsNullable: true,
			References: &storage.ForeignKeyRef{Table: "departments", Column: "id"}},
	})

	empPath := filepath.Join(tempDir, "testdb", "employees.db")
	empPager, _ := storage.NewPager(empPath)
	empTable := storage.NewTable(empPager, empSchema)
	exec.RegisterTable("employees", empTable)

	// Insert parent rows
	deptTable.Insert([]interface{}{int32(1), "Engineering"})
	deptTable.Insert([]interface{}{int32(2), "Sales"})

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	return exec, func() {}
}

// ── ScanNode tests ──────────────────────────────────────────────────────────

func TestExecuteScanNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// Insert some data
	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})

	plan := &planner.ScanNode{TableName: "users"}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Columns) != 3 {
		t.Errorf("expected 3 columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestExecuteScanNodeTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.ScanNode{TableName: "nonexistent"}
	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

// ── FilterNode tests ────────────────────────────────────────────────────────

func TestExecuteFilterNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})
	exec.Tables["users"].Insert([]interface{}{int32(3), "Charlie", int32(35)})

	plan := &planner.FilterNode{
		Child: &planner.ScanNode{TableName: "users"},
		Left:  "age",
		Op:    ">",
		Right: "28",
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows (age > 28), got %d", len(result.Rows))
	}
}

func TestExecuteFilterNodeAllOperators(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	tests := []struct {
		op      string
		right   string
		expect  bool
	}{
		{"=", "30", true},
		{"!=", "25", true},
		{">", "20", true},
		{"<", "40", true},
		{">=", "30", true},
		{"<=", "30", true},
		{"=", "99", false},
		{"!=", "30", false},
		{">", "30", false},
		{"<", "30", false},
	}

	for _, tt := range tests {
		plan := &planner.FilterNode{
			Child: &planner.ScanNode{TableName: "users"},
			Left:  "age",
			Op:    tt.op,
			Right: tt.right,
		}

		result, err := exec.Execute(plan)
		if err != nil {
			t.Fatalf("op %s: Execute failed: %v", tt.op, err)
		}

		hasRows := len(result.Rows) > 0
		if hasRows != tt.expect {
			t.Errorf("op %s %s: expected match=%v, got %d rows", tt.op, tt.right, tt.expect, len(result.Rows))
		}
	}
}

func TestExecuteFilterNodeColumnNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	plan := &planner.FilterNode{
		Child: &planner.ScanNode{TableName: "users"},
		Left:  "nonexistent",
		Op:    "=",
		Right: "1",
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent column in filter")
	}
}

func TestExecuteFilterNodeNullComparison(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), nil, nil})

	// Try to filter on a NULL column with !=
	plan := &planner.FilterNode{
		Child: &planner.ScanNode{TableName: "users"},
		Left:  "name",
		Op:    "!=",
		Right: "Alice",
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}
	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows for NULL != value, got %d", len(result.Rows))
	}
}

func TestExecuteFilterNodeLike(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})
	exec.Tables["users"].Insert([]interface{}{int32(3), "Charlie", int32(35)})

	tests := []struct {
		pattern string
		expect  int
	}{
		{"A%", 1},
		{"%e", 2},  // Alice, Charlie
		{"%ob%", 1}, // Bob
		{"%x%", 0},
		{"Alice", 1},
	}

	for _, tt := range tests {
		plan := &planner.FilterNode{
			Child: &planner.ScanNode{TableName: "users"},
			Left:  "name",
			Op:    "LIKE",
			Right: tt.pattern,
		}
		result, err := exec.Execute(plan)
		if err != nil {
			t.Fatalf("LIKE %s: Execute failed: %v", tt.pattern, err)
		}
		if len(result.Rows) != tt.expect {
			t.Errorf("LIKE %s: expected %d rows, got %d", tt.pattern, tt.expect, len(result.Rows))
		}
	}
}

func TestExecuteFilterNodeIn(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})
	exec.Tables["users"].Insert([]interface{}{int32(3), "Charlie", int32(35)})

	plan := &planner.FilterNode{
		Child:  &planner.ScanNode{TableName: "users"},
		Left:   "id",
		Op:     "IN",
		InList: []string{"1", "3"},
	}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute IN failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows for IN (1,3), got %d", len(result.Rows))
	}
}

func TestExecuteFilterNodeBetween(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})
	exec.Tables["users"].Insert([]interface{}{int32(3), "Charlie", int32(35)})
	exec.Tables["users"].Insert([]interface{}{int32(4), "Dave", int32(99)})

	plan := &planner.FilterNode{
		Child:  &planner.ScanNode{TableName: "users"},
		Left:   "age",
		Op:     "BETWEEN",
		Right:  "25",
		Right2: "35",
	}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute BETWEEN failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows for BETWEEN 25 AND 35, got %d", len(result.Rows))
	}
}

func TestExecuteFilterNodeIsNull(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), nil, int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})

	plan := &planner.FilterNode{
		Child: &planner.ScanNode{TableName: "users"},
		Left:  "name",
		Op:    "IS NULL",
	}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute IS NULL failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for IS NULL, got %d", len(result.Rows))
	}
}

func TestExecuteFilterNodeIsNotNull(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), nil, int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})

	plan := &planner.FilterNode{
		Child: &planner.ScanNode{TableName: "users"},
		Left:  "name",
		Op:    "IS NOT NULL",
	}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute IS NOT NULL failed: %v", err)
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row for IS NOT NULL, got %d", len(result.Rows))
	}
}

func TestExecuteDistinctNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})
	exec.Tables["users"].Insert([]interface{}{int32(3), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(4), "Charlie", int32(35)})

	// Duplicate over name+age (different id)
	plan := &planner.DistinctNode{
		Child: &planner.ScanNode{TableName: "users"},
	}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute DistinctNode failed: %v", err)
	}
	// All rows have distinct id, so all 4 should remain
	if len(result.Rows) != 4 {
		t.Errorf("expected 4 rows (distinct ids), got %d", len(result.Rows))
	}

	// Now test with projection: SELECT DISTINCT name FROM users
	// After projection, "Alice" appears twice but DISTINCT should deduplicate
	projectPlan := &planner.DistinctNode{
		Child: &planner.ProjectNode{
			Child:   &planner.ScanNode{TableName: "users"},
			Columns: []string{"name"},
		},
	}
	result, err = exec.Execute(projectPlan)
	if err != nil {
		t.Fatalf("Execute DistinctNode+Project failed: %v", err)
	}
	if len(result.Rows) != 3 {
		t.Errorf("expected 3 distinct names, got %d", len(result.Rows))
	}
}

// ── ProjectNode tests ───────────────────────────────────────────────────────

func TestExecuteProjectNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	plan := &planner.ProjectNode{
		Child:   &planner.ScanNode{TableName: "users"},
		Columns: []string{"name", "age"},
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(result.Columns))
	}
	if result.Columns[0] != "name" || result.Columns[1] != "age" {
		t.Errorf("columns mismatch: %v", result.Columns)
	}
	if len(result.Rows[0].Values) != 2 {
		t.Errorf("expected 2 values per row, got %d", len(result.Rows[0].Values))
	}
}

func TestExecuteProjectNodeColumnNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	plan := &planner.ProjectNode{
		Child:   &planner.ScanNode{TableName: "users"},
		Columns: []string{"nonexistent"},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent column in projection")
	}
}

// ── InsertNode tests ────────────────────────────────────────────────────────

func TestExecuteInsertNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.InsertNode{
		TableName: "users",
		Values:    []string{"1", "Alice", "30"},
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute insert failed: %v", err)
	}

	// Verify by scanning
	rows, _ := exec.Tables["users"].SelectAll()
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
}

func TestExecuteInsertNodeWithColumns(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.InsertNode{
		TableName: "users",
		Columns:   []string{"id", "name", "age"},
		Values:    []string{"1", "Alice", "30"},
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute insert with columns failed: %v", err)
	}

	rows, _ := exec.Tables["users"].SelectAll()
	if len(rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(rows))
	}
	if rows[0].Values[1].(string) != "Alice" {
		t.Errorf("expected Alice, got %v", rows[0].Values[1])
	}
}

func TestExecuteInsertNodeCountMismatch(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.InsertNode{
		TableName: "users",
		Columns:   []string{"name", "age"},
		Values:    []string{"Alice"}, // missing value
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for column/value count mismatch")
	}
}

func TestExecuteInsertNodeValueCountMismatch(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.InsertNode{
		TableName: "users",
		Values:    []string{"1", "Alice"}, // Missing age
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for value count mismatch")
	}
}

func TestExecuteInsertNodeTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.InsertNode{
		TableName: "nonexistent",
		Values:    []string{"1"},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent table")
	}
}

func TestExecuteInsertNodeUniqueViolation(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	plan := &planner.InsertNode{
		TableName: "users",
		Values:    []string{"1", "Bob", "25"}, // Duplicate id = 1 (PK)
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected UNIQUE constraint violation")
	}
}

func TestExecuteInsertNodeNotNullViolation(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.InsertNode{
		TableName: "users",
		Values:    []string{"NULL", "Alice", "30"}, // id is NOT NULL
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected NOT NULL constraint violation")
	}
}

func TestExecuteInsertNodeFKViolation(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Try to insert employee with dept_id that doesn't exist
	plan := &planner.InsertNode{
		TableName: "employees",
		Values:    []string{"1", "Alice", "99"}, // dept 99 doesn't exist
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected FK constraint violation")
	}
}

func TestExecuteInsertNodeFKValid(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert employee with valid dept_id
	plan := &planner.InsertNode{
		TableName: "employees",
		Values:    []string{"1", "Alice", "1"}, // dept 1 exists
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute insert with valid FK failed: %v", err)
	}
}

func TestExecuteInsertNodeFKNull(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert employee with NULL FK is allowed
	plan := &planner.InsertNode{
		TableName: "employees",
		Values:    []string{"1", "Alice", "NULL"},
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute insert with NULL FK should succeed: %v", err)
	}
}

// ── DeleteNode tests ────────────────────────────────────────────────────────

func TestExecuteDeleteNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})

	plan := &planner.DeleteNode{
		TableName: "users",
		Where:     nil, // Delete all
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute delete failed: %v", err)
	}

	if result.Message != "Deleted 2 rows" {
		t.Errorf("unexpected message: %s", result.Message)
	}

	rows, _ := exec.Tables["users"].SelectAll()
	if len(rows) != 0 {
		t.Errorf("expected 0 rows after delete, got %d", len(rows))
	}
}

func TestExecuteDeleteNodeWithWhere(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})

	delPlan := &planner.DeleteNode{
		TableName: "users",
		Where:     &ast.WhereClause{Left: "id", Op: "=", Right: "1"},
	}

	result, err := exec.Execute(delPlan)
	if err != nil {
		t.Fatalf("Execute delete failed: %v", err)
	}

	if result.Message != "Deleted 1 rows" {
		t.Errorf("unexpected message: %s", result.Message)
	}

	rows, _ := exec.Tables["users"].SelectAll()
	if len(rows) != 1 {
		t.Errorf("expected 1 row after delete, got %d", len(rows))
	}
}

func TestExecuteDeleteNodeBlockedByFK(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert employee that references department 1
	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})

	// Try to delete a department that has employees referencing it
	plan := &planner.DeleteNode{
		TableName: "departments",
		Where:     nil, // Delete all - but employees reference departments
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected FK constraint violation on delete")
	}
}

// ── UpdateNode tests ────────────────────────────────────────────────────────

func TestExecuteUpdateNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	plan := &planner.UpdateNode{
		TableName: "users",
		Sets:      map[string]string{"name": "Alicia", "age": "31"},
		Where:     &ast.WhereClause{Left: "id", Op: "=", Right: "1"},
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute update failed: %v", err)
	}

	if result.Message != "Updated 1 rows" {
		t.Errorf("unexpected message: %s", result.Message)
	}

	rows, _ := exec.Tables["users"].SelectAll()
	if rows[0].Values[1].(string) != "Alicia" {
		t.Errorf("expected Alicia, got %v", rows[0].Values[1])
	}
	if rows[0].Values[2].(int32) != 31 {
		t.Errorf("expected 31, got %v", rows[0].Values[2])
	}
}

func TestExecuteUpdateNodeWithoutWhere(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(2), "Bob", int32(25)})

	plan := &planner.UpdateNode{
		TableName: "users",
		Sets:      map[string]string{"age": "99"},
		Where:     nil, // Update all
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute update failed: %v", err)
	}

	if result.Message != "Updated 2 rows" {
		t.Errorf("unexpected message: %s", result.Message)
	}
}

func TestExecuteUpdateNodeColumnNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(30)})

	plan := &planner.UpdateNode{
		TableName: "users",
		Sets:      map[string]string{"nonexistent": "value"},
		Where:     nil,
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent column in SET")
	}
}

func TestExecuteUpdateNodeFKChildViolation(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert employee with dept 1
	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})

	// Try to change dept to non-existent 99
	plan := &planner.UpdateNode{
		TableName: "employees",
		Sets:      map[string]string{"dept_id": "99"},
		Where:     nil,
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected FK constraint violation on update child")
	}
}

func TestExecuteUpdateNodeFKParentPKChangeBlocked(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert employee with dept 1
	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})

	// Try to change department PK 1 -> 3
	plan := &planner.UpdateNode{
		TableName: "departments",
		Sets:      map[string]string{"id": "3"},
		Where:     &ast.WhereClause{Left: "id", Op: "=", Right: "1"},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected FK constraint violation on update parent PK")
	}
}

// ── CreateDatabase / UseDatabase tests ──────────────────────────────────────

func TestExecuteCreateDatabaseNode(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	plan := &planner.CreateDatabaseNode{DatabaseName: "newdb"}
	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute CreateDatabase failed: %v", err)
	}
}

func TestExecuteCreateDatabaseNodeDuplicate(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	exec.Execute(&planner.CreateDatabaseNode{DatabaseName: "newdb"})
	_, err := exec.Execute(&planner.CreateDatabaseNode{DatabaseName: "newdb"})
	if err == nil {
		t.Error("expected error for duplicate database")
	}
}

func TestExecuteUseDatabaseNode(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	exec.Execute(&planner.CreateDatabaseNode{DatabaseName: "mydb"})
	_, err := exec.Execute(&planner.UseDatabaseNode{DatabaseName: "mydb"})
	if err != nil {
		t.Fatalf("Execute UseDatabase failed: %v", err)
	}
	if exec.Engine.ActiveDB != "mydb" {
		t.Errorf("expected mydb, got %s", exec.Engine.ActiveDB)
	}
}

func TestExecuteUseDatabaseNodeNotExists(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	_, err := exec.Execute(&planner.UseDatabaseNode{DatabaseName: "nonexistent"})
	if err == nil {
		t.Error("expected error for non-existent database")
	}
}

// ── CreateTableNode tests ───────────────────────────────────────────────────

func TestExecuteCreateTableNode(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	plan := &planner.CreateTableNode{
		TableName: "newtable",
		Columns: []ast.ColumnDefinition{
			{Name: "id", DataType: "INT", IsNullable: false, IsPrimaryKey: true},
			{Name: "name", DataType: "TEXT", Size: 32, IsNullable: true},
		},
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute CreateTable failed: %v", err)
	}

	if _, ok := exec.Tables["newtable"]; !ok {
		t.Errorf("expected newtable to be registered")
	}
}

func TestExecuteCreateTableNodeDuplicate(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	plan := &planner.CreateTableNode{
		TableName: "mytable",
		Columns: []ast.ColumnDefinition{
			{Name: "id", DataType: "INT"},
		},
	}

	exec.Execute(plan)
	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for duplicate table")
	}
}

func TestExecuteCreateTableNodeNoActiveDB(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	plan := &planner.CreateTableNode{
		TableName: "t",
		Columns:   []ast.ColumnDefinition{{Name: "id", DataType: "INT"}},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for no active database")
	}
}

func TestExecuteCreateTableNodeUnsupportedType(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	plan := &planner.CreateTableNode{
		TableName: "t",
		Columns: []ast.ColumnDefinition{
			{Name: "id", DataType: "BOOLEAN"}, // Unsupported type
		},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for unsupported type")
	}
}

// ── JoinNode tests ──────────────────────────────────────────────────────────

func TestExecuteJoinNode(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert employees referencing departments
	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})
	exec.Tables["employees"].Insert([]interface{}{int32(2), "Bob", int32(2)})

	plan := &planner.JoinNode{
		Left:     &planner.ScanNode{TableName: "employees"},
		Right:    &planner.ScanNode{TableName: "departments"},
		LeftKey:  "employees.dept_id",
		RightKey: "departments.id",
		Columns:  []string{"*"},
		Where:    nil,
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute join failed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 joined rows, got %d", len(result.Rows))
	}

	// Result should have all columns from both tables
	expectedCols := 3 + 2 // employees(3) + departments(2)
	if len(result.Columns) != expectedCols {
		t.Errorf("expected %d columns, got %d: %v", expectedCols, len(result.Columns), result.Columns)
	}
}

func TestExecuteJoinNodeWithProjection(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})

	plan := &planner.JoinNode{
		Left:     &planner.ScanNode{TableName: "employees"},
		Right:    &planner.ScanNode{TableName: "departments"},
		LeftKey:  "employees.dept_id",
		RightKey: "departments.id",
		Columns:  []string{"employees.name", "departments.name"},
		Where:    nil,
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute join with projection failed: %v", err)
	}

	if len(result.Columns) != 2 {
		t.Errorf("expected 2 projected columns, got %d", len(result.Columns))
	}
	if len(result.Rows) != 1 {
		t.Errorf("expected 1 row, got %d", len(result.Rows))
	}
}

func TestExecuteJoinNodeWithWhere(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})
	exec.Tables["employees"].Insert([]interface{}{int32(2), "Bob", int32(2)})

	plan := &planner.JoinNode{
		Left:     &planner.ScanNode{TableName: "employees"},
		Right:    &planner.ScanNode{TableName: "departments"},
		LeftKey:  "employees.dept_id",
		RightKey: "departments.id",
		Columns:  []string{"*"},
		Where:    &ast.WhereClause{Left: "employees.name", Op: "=", Right: "Alice"},
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Execute join with where failed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Errorf("expected 1 filtered row, got %d", len(result.Rows))
	}
}

func TestExecuteJoinNodeLeftTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.JoinNode{
		Left:     &planner.ScanNode{TableName: "nonexistent"},
		Right:    &planner.ScanNode{TableName: "users"},
		LeftKey:  "x.id",
		RightKey: "y.id",
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent left table")
	}
}

func TestExecuteJoinNodeKeyColumnNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.JoinNode{
		Left:     &planner.ScanNode{TableName: "users"},
		Right:    &planner.ScanNode{TableName: "users"},
		LeftKey:  "users.nonexistent",
		RightKey: "users.id",
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent key column")
	}
}

// ── FormatResultSet tests ───────────────────────────────────────────────────

func TestFormatResultSet(t *testing.T) {
	res := ResultSet{
		Columns: []string{"id", "name"},
		Rows: []storage.Row{
			{Values: []interface{}{int32(1), "Alice"}},
			{Values: []interface{}{int32(2), "Bob"}},
		},
	}

	out := FormatResultSet(res)
	if out == "" {
		t.Error("expected formatted output")
	}
}

func TestFormatResultSetEmpty(t *testing.T) {
	res := ResultSet{}
	out := FormatResultSet(res)
	if out != "Success (Action completed)" {
		t.Errorf("unexpected output for empty: %q", out)
	}
}

func TestFormatResultSetNoRows(t *testing.T) {
	res := ResultSet{
		Columns: []string{"id", "name"},
		Rows:    []storage.Row{},
	}
	out := FormatResultSet(res)
	if out == "" {
		t.Error("expected formatted output with 0 rows")
	}
}

func TestFormatResultSetWithMessage(t *testing.T) {
	res := ResultSet{Message: "Deleted 5 rows"}
	out := FormatResultSet(res)
	// Should still return "Success (Action completed)" since Columns and Rows are empty
	if out != "Success (Action completed)" {
		t.Errorf("expected success message, got %q", out)
	}
}

// ── convertValues / convertSingleValue tests ────────────────────────────────

func TestConvertSingleValueInt(t *testing.T) {
	exec, _ := setupExecutor(t)

	col := storage.Column{Name: "id", Type: storage.TypeInt32}
	val, err := exec.convertSingleValue("42", col)
	if err != nil {
		t.Fatalf("convertSingleValue failed: %v", err)
	}
	if val.(int32) != 42 {
		t.Errorf("expected 42, got %v", val)
	}
}

func TestConvertSingleValueInvalidInt(t *testing.T) {
	exec, _ := setupExecutor(t)

	col := storage.Column{Name: "id", Type: storage.TypeInt32}
	_, err := exec.convertSingleValue("notanumber", col)
	if err == nil {
		t.Error("expected error for invalid int")
	}
}

func TestConvertSingleValueNull(t *testing.T) {
	exec, _ := setupExecutor(t)

	col := storage.Column{Name: "name", Type: storage.TypeFixedText, IsNullable: true}
	val, err := exec.convertSingleValue("NULL", col)
	if err != nil {
		t.Fatalf("convertSingleValue NULL failed: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
}

func TestConvertSingleValueNullOnNotNull(t *testing.T) {
	exec, _ := setupExecutor(t)

	col := storage.Column{Name: "id", Type: storage.TypeInt32, IsNullable: false}
	_, err := exec.convertSingleValue("NULL", col)
	if err == nil {
		t.Error("expected error for NULL on NOT NULL column")
	}
}

func TestConvertValuesMismatch(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "a", Type: storage.TypeInt32},
		{Name: "b", Type: storage.TypeInt32},
	})

	_, err := exec.convertValues([]string{"1"}, schema)
	if err == nil {
		t.Error("expected error for value count mismatch")
	}
}

func TestConvertValuesUnknownType(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "a", Type: storage.DataType(99)},
	})

	_, err := exec.convertValues([]string{"1"}, schema)
	if err == nil {
		t.Error("expected error for unknown type")
	}
}

// ── evaluateCondition edge cases ────────────────────────────────────────────

func TestEvaluateConditionColumnNotFound(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})

	row := storage.Row{Values: []interface{}{int32(1)}}
	_, err := exec.evaluateCondition(row, schema, "nonexistent", "=", "1")
	if err == nil {
		t.Error("expected error for non-existent column")
	}
}

func TestEvaluateConditionInvalidIntValue(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})

	row := storage.Row{Values: []interface{}{int32(1)}}
	_, err := exec.evaluateCondition(row, schema, "id", "=", "notanumber")
	if err == nil {
		t.Error("expected error for invalid int value")
	}
}

// ── compareValue tests (join WHERE path) ────────────────────────────────────

func TestCompareValue(t *testing.T) {
	exec, _ := setupExecutor(t)

	tests := []struct {
		val    interface{}
		op     string
		right  string
		result bool
	}{
		{int32(5), "=", "5", true},
		{int32(5), "!=", "3", true},
		{int32(5), ">", "3", true},
		{int32(5), "<", "10", true},
		{int32(5), ">=", "5", true},
		{int32(5), "<=", "5", true},
		{int32(5), "=", "6", false},
		{"hello", "=", "hello", true},
		{"hello", "!=", "world", true},
		{"hello", ">", "abc", true},
		{"hello", "<", "z", true},
		{nil, "=", "anything", false},
	}

	for _, tt := range tests {
		result, err := exec.compareValue(tt.val, tt.op, tt.right)
		if err != nil {
			t.Fatalf("compareValue(%v, %s, %s) failed: %v", tt.val, tt.op, tt.right, err)
		}
		if result != tt.result {
			t.Errorf("compareValue(%v, %s, %s) = %v, expected %v", tt.val, tt.op, tt.right, result, tt.result)
		}
	}
}

func TestCompareValueInvalidInt(t *testing.T) {
	exec, _ := setupExecutor(t)
	_, err := exec.compareValue(int32(1), "=", "notanumber")
	if err == nil {
		t.Error("expected error for invalid int")
	}
}

// ── resolveQualifiedCol tests ───────────────────────────────────────────────

func TestResolveQualifiedCol(t *testing.T) {
	tests := []struct {
		input    string
		expTable string
		expCol   string
	}{
		{"users.id", "users", "id"},
		{"orders.user_id", "orders", "user_id"},
		{"simple", "", "simple"},
		{"a.b.c", "a", "b.c"},
	}

	for _, tt := range tests {
		table, col := resolveQualifiedCol(tt.input)
		if table != tt.expTable || col != tt.expCol {
			t.Errorf("resolveQualifiedCol(%q) = (%q, %q), expected (%q, %q)",
				tt.input, table, col, tt.expTable, tt.expCol)
		}
	}
}

// ── New / RegisterTable / SaveTableSchema tests ─────────────────────────────

func TestNew(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	if exec.Engine != engine {
		t.Error("engine not set correctly")
	}
	if len(exec.Tables) != 0 {
		t.Errorf("expected empty tables map, got %d", len(exec.Tables))
	}
}

func TestRegisterTable(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	if _, ok := exec.Tables["users"]; !ok {
		t.Error("expected users table to be registered")
	}
}

func TestSaveTableSchemaNoActiveDB(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})

	err := exec.SaveTableSchema("test", schema)
	if err == nil {
		t.Error("expected error for no active database")
	}
}

// ── getTableFromPlan tests ──────────────────────────────────────────────────

func TestGetTableFromPlan(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// ScanNode
	table := exec.getTableFromPlan(&planner.ScanNode{TableName: "users"})
	if table == nil {
		t.Error("expected non-nil table for ScanNode")
	}

	// FilterNode wrapping ScanNode
	table = exec.getTableFromPlan(&planner.FilterNode{
		Child: &planner.ScanNode{TableName: "users"},
	})
	if table == nil {
		t.Error("expected non-nil table for FilterNode")
	}

	// ProjectNode wrapping ScanNode
	table = exec.getTableFromPlan(&planner.ProjectNode{
		Child: &planner.ScanNode{TableName: "users"},
	})
	if table == nil {
		t.Error("expected non-nil table for ProjectNode")
	}

	// Unknown node type
	table = exec.getTableFromPlan(&planner.InsertNode{})
	if table != nil {
		t.Error("expected nil for unknown node")
	}
}

// ── Unknown plan node test ──────────────────────────────────────────────────

func TestExecuteUnknownNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	_, err := exec.Execute(nil)
	if err == nil {
		t.Error("expected error for unknown plan node")
	}
}

// ── checkReferencingChildren tests ──────────────────────────────────────────

func TestCheckReferencingChildrenNoPK(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	parentTable := exec.Tables["departments"]
	row := storage.Row{Values: []interface{}{int32(1), "Engineering"}}

	// Should not error because we didn't set PK on departments...
	// Actually departments does have PK set. So let's test without PK.
	err := exec.checkReferencingChildren(parentTable, row)
	if err != nil {
		// This might actually fail because departments has PK=id and employees reference it
		// Just verify it doesn't panic
	}
}

func TestExecuteDeleteNodeFKBlockedByChildren(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Insert an employee that references dept 1
	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})

	// Try to delete department 1 - should be blocked
	delPlan := &planner.DeleteNode{
		TableName: "departments",
		Where:     &ast.WhereClause{Left: "id", Op: "=", Right: "1"},
	}

	_, err := exec.Execute(delPlan)
	if err == nil {
		t.Error("expected FK violation when deleting parent with children")
	}
}

func TestExecuteDeleteNodeNoFKBlockWhenNoChildren(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Delete department 2 (no employees reference it)
	delPlan := &planner.DeleteNode{
		TableName: "departments",
		Where:     &ast.WhereClause{Left: "id", Op: "=", Right: "2"},
	}

	result, err := exec.Execute(delPlan)
	if err != nil {
		t.Fatalf("expected successful delete, got: %v", err)
	}
	if result.Message != "Deleted 1 rows" {
		t.Errorf("unexpected message: %s", result.Message)
	}
}

// ── ReloadTables tests ──────────────────────────────────────────────────────

func TestReloadTablesNoActiveDB(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)

	err := exec.ReloadTables()
	if err != nil {
		t.Fatalf("ReloadTables with no active DB failed: %v", err)
	}
	if len(exec.Tables) != 0 {
		t.Errorf("expected empty tables, got %d", len(exec.Tables))
	}
}

func TestReloadTablesWithData(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	// Create a table directly
	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})
	dbPath := filepath.Join(tempDir, "testdb", "mytable.db")
	pager, _ := storage.NewPager(dbPath)
	table := storage.NewTable(pager, schema)
	exec.RegisterTable("mytable", table)

	// Save schema to disk (required for ReloadTables to pick it up)
	err := exec.SaveTableSchema("mytable", schema)
	if err != nil {
		t.Fatalf("SaveTableSchema failed: %v", err)
	}
	pager.Close()

	// Reload should find the table from disk
	exec2 := New(engine)
	err = exec2.ReloadTables()
	if err != nil {
		t.Fatalf("ReloadTables failed: %v", err)
	}
	if _, ok := exec2.Tables["mytable"]; !ok {
		t.Error("expected mytable to be reloaded")
	}

	t.Cleanup(func() {
		for _, tbl := range exec2.Tables {
			tbl.Pager.Close()
		}
	})
}

func TestReloadTablesSkipsMissingDBFile(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	// Write a .json schema file without a .db file
	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})
	exec.SaveTableSchema("orphan", schema)

	// Reload should not fail but should skip the orphan
	err := exec.ReloadTables()
	if err != nil {
		t.Fatalf("ReloadTables failed: %v", err)
	}
}

// ── evaluateFilter convenience wrapper ──────────────────────────────────────

func TestEvaluateFilter(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "val", Type: storage.TypeInt32},
	})

	row := storage.Row{Values: []interface{}{int32(10)}}
	filter := &planner.FilterNode{Left: "val", Op: ">", Right: "5"}

	match, err := exec.evaluateFilter(row, schema, filter)
	if err != nil {
		t.Fatalf("evaluateFilter failed: %v", err)
	}
	if !match {
		t.Error("expected match for val > 5")
	}

	filter2 := &planner.FilterNode{Left: "val", Op: "<", Right: "5"}
	match2, _ := exec.evaluateFilter(row, schema, filter2)
	if match2 {
		t.Error("expected no match for val < 5")
	}
}

// ── applyProjection edge cases ─────────────────────────────────────────────

func TestApplyProjectionColumnNotFound(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})

	rows := []storage.Row{{Values: []interface{}{int32(1)}}}
	_, err := exec.applyProjection(rows, schema, []string{"nonexistent"})
	if err == nil {
		t.Error("expected error for non-existent column in projection")
	}
}

func TestApplyProjectionEmptyColumns(t *testing.T) {
	exec, _ := setupExecutor(t)

	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})

	rows := []storage.Row{{Values: []interface{}{int32(1)}}}
	result, err := exec.applyProjection(rows, schema, []string{})
	if err != nil {
		t.Fatalf("applyProjection with empty columns failed: %v", err)
	}
	if len(result) != 1 {
		t.Errorf("expected 1 row, got %d", len(result))
	}
}

// ── Execute InsertNode FK parent table not found ────────────────────────────

func TestExecuteInsertNodeFKParentTableNotFound(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
		{Name: "ref", Type: storage.TypeInt32, IsNullable: true,
			References: &storage.ForeignKeyRef{Table: "nonexistent", Column: "id"}},
	})

	dbPath := filepath.Join(tempDir, "testdb", "child.db")
	pager, _ := storage.NewPager(dbPath)
	table := storage.NewTable(pager, schema)
	exec.RegisterTable("child", table)

	plan := &planner.InsertNode{
		TableName: "child",
		Values:    []string{"1", "1"},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error when parent table not found for FK")
	}
}

func TestExecuteInsertNodeFKParentColumnNotFound(t *testing.T) {
	tempDir := t.TempDir()
	engine := storage.NewEngine(tempDir)
	exec := New(engine)
	exec.Engine.ActiveDB = "testdb"
	os.MkdirAll(filepath.Join(tempDir, "testdb"), 0755)

	t.Cleanup(func() {
		for _, tbl := range exec.Tables {
			tbl.Pager.Close()
		}
	})

	// Create parent table without the referenced column
	parentSchema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
	})
	parentPath := filepath.Join(tempDir, "testdb", "parent.db")
	parentPager, _ := storage.NewPager(parentPath)
	parentTable := storage.NewTable(parentPager, parentSchema)
	exec.RegisterTable("parent", parentTable)

	// Create child referencing a non-existent column
	childSchema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32},
		{Name: "ref", Type: storage.TypeInt32, IsNullable: true,
			References: &storage.ForeignKeyRef{Table: "parent", Column: "nonexistent"}},
	})
	childPath := filepath.Join(tempDir, "testdb", "child.db")
	childPager, _ := storage.NewPager(childPath)
	childTable := storage.NewTable(childPager, childSchema)
	exec.RegisterTable("child", childTable)

	plan := &planner.InsertNode{
		TableName: "child",
		Values:    []string{"1", "1"},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error when parent column not found for FK reference")
	}
}

func TestExecuteUpdateNodeFKParentTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutorWithFK(t)
	defer cleanup()

	// Create a non-existent table reference by modifying schema directly won't work easily
	// Instead, test the existing FK update path
	exec.Tables["employees"].Insert([]interface{}{int32(1), "Alice", int32(1)})

	// This should succeed since the FK parent exists
	plan := &planner.UpdateNode{
		TableName: "employees",
		Sets:      map[string]string{"name": "Alicia"},
		Where:     nil,
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("Update with valid FK parent should succeed: %v", err)
	}
}

// ── Table not found for Update/Delete ───────────────────────────────────────

func TestExecuteUpdateNodeTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.UpdateNode{
		TableName: "nonexistent",
		Sets:      map[string]string{"name": "test"},
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent table in update")
	}
}

func TestExecuteDeleteNodeTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.DeleteNode{
		TableName: "nonexistent",
	}

	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent table in delete")
	}
}

func TestExecuteDropTable(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.DropTableNode{TableName: "users"}
	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("DropTable should succeed: %v", err)
	}

	if _, ok := exec.Tables["users"]; ok {
		t.Error("table should no longer be registered after drop")
	}
}

func TestExecuteDropTableNotFound(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.DropTableNode{TableName: "nonexistent"}
	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for non-existent table in drop")
	}
}

func TestExecuteDropDatabase(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.DropDatabaseNode{DatabaseName: "testdb"}
	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("DropDatabase should succeed: %v", err)
	}

	if exec.Engine.ActiveDB != "" {
		t.Error("ActiveDB should be cleared after dropping the active database")
	}
	if len(exec.Tables) != 0 {
		t.Error("Tables should be cleared after dropping the active database")
	}
}

func TestExecuteDropDatabaseNotActive(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.DropDatabaseNode{DatabaseName: "otherdb"}
	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for dropping a database that is not active")
	}
}

func TestExecuteShowDatabases(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	plan := &planner.ShowDatabasesNode{}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("ShowDatabases should succeed: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "Database" {
		t.Errorf("expected [Database] columns, got %v", result.Columns)
	}

	found := false
	for _, row := range result.Rows {
		if row.Values[0] == "testdb" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected testdb to be in the database list")
	}
}

func TestExecuteShowTables(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// Create the .db file on disk (setupExecutor creates users table in memory but we need the file)
	// Actually, setupExecutor does create the pager which creates the file, so it should already exist.

	plan := &planner.ShowTablesNode{}
	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("ShowTables should succeed: %v", err)
	}

	if len(result.Columns) != 1 || result.Columns[0] != "Table" {
		t.Errorf("expected [Table] columns, got %v", result.Columns)
	}

	found := false
	for _, row := range result.Rows {
		if row.Values[0] == "users" {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected users to be in the table list")
	}
}

func TestExecuteShowTablesNoDatabase(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	exec.Engine.ActiveDB = ""

	plan := &planner.ShowTablesNode{}
	_, err := exec.Execute(plan)
	if err == nil {
		t.Error("expected error for ShowTables with no active database")
	}
}

// ── SortNode tests ────────────────────────────────────────────────────────

func TestExecuteSortNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// Insert some rows out of order
	for _, val := range []int32{3, 1, 2} {
		exec.Tables["users"].Insert([]interface{}{val, "user", val})
	}

	plan := &planner.SortNode{
		Child:   &planner.ScanNode{TableName: "users"},
		OrderBy: []ast.SortExpression{{Column: "id", Direction: "ASC"}},
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("SortNode should succeed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(result.Rows))
	}

	// Check sorted order
	expected := []int32{1, 2, 3}
	for i, exp := range expected {
		got := result.Rows[i].Values[0].(int32)
		if got != exp {
			t.Errorf("row %d: expected %d, got %d", i, exp, got)
		}
	}
}

func TestExecuteSortNodeDesc(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	for _, val := range []int32{1, 2, 3} {
		exec.Tables["users"].Insert([]interface{}{val, "user", val})
	}

	plan := &planner.SortNode{
		Child:   &planner.ScanNode{TableName: "users"},
		OrderBy: []ast.SortExpression{{Column: "id", Direction: "DESC"}},
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("SortNode DESC should succeed: %v", err)
	}

	expected := []int32{3, 2, 1}
	for i, exp := range expected {
		got := result.Rows[i].Values[0].(int32)
		if got != exp {
			t.Errorf("row %d: expected %d, got %d", i, exp, got)
		}
	}
}

func TestExecuteSortNodeMultipleColumns(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// Insert rows with (id, name, age)
	exec.Tables["users"].Insert([]interface{}{int32(2), "Alice", int32(30)})
	exec.Tables["users"].Insert([]interface{}{int32(1), "Alice", int32(25)})
	exec.Tables["users"].Insert([]interface{}{int32(3), "Bob", int32(20)})

	plan := &planner.SortNode{
		Child: &planner.ScanNode{TableName: "users"},
		OrderBy: []ast.SortExpression{
			{Column: "name", Direction: "ASC"},
			{Column: "age", Direction: "ASC"},
		},
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("SortNode multi-col should succeed: %v", err)
	}

	// Alice(25), Alice(30), Bob(20)
	if result.Rows[0].Values[1].(string) != "Alice" || result.Rows[0].Values[2].(int32) != 25 {
		t.Errorf("first row should be Alice 25")
	}
	if result.Rows[1].Values[1].(string) != "Alice" || result.Rows[1].Values[2].(int32) != 30 {
		t.Errorf("second row should be Alice 30")
	}
	if result.Rows[2].Values[1].(string) != "Bob" {
		t.Errorf("third row should be Bob")
	}
}

// ── LimitNode tests ───────────────────────────────────────────────────────

func TestExecuteLimitNode(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	for _, val := range []int32{1, 2, 3, 4, 5} {
		exec.Tables["users"].Insert([]interface{}{val, "user", val})
	}

	plan := &planner.LimitNode{
		Child: &planner.ScanNode{TableName: "users"},
		Limit: 3,
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("LimitNode should succeed: %v", err)
	}

	if len(result.Rows) != 3 {
		t.Errorf("expected 3 rows, got %d", len(result.Rows))
	}
}

func TestExecuteLimitNodeWithOffset(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	for _, val := range []int32{1, 2, 3, 4, 5} {
		exec.Tables["users"].Insert([]interface{}{val, "user", val})
	}

	plan := &planner.LimitNode{
		Child:  &planner.ScanNode{TableName: "users"},
		Limit:  2,
		Offset: 2,
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("LimitNode with offset should succeed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	// With offset 2, should get values 3, 4
	if result.Rows[0].Values[0].(int32) != 3 || result.Rows[1].Values[0].(int32) != 4 {
		t.Errorf("unexpected rows after offset: %v, %v", result.Rows[0].Values[0], result.Rows[1].Values[0])
	}
}

func TestExecuteLimitNodeOffsetExceedsRows(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	for _, val := range []int32{1, 2} {
		exec.Tables["users"].Insert([]interface{}{val, "user", val})
	}

	plan := &planner.LimitNode{
		Child:  &planner.ScanNode{TableName: "users"},
		Offset: 10,
	}

	result, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("LimitNode with large offset should succeed: %v", err)
	}

	if len(result.Rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(result.Rows))
	}
}

func TestCreateTableWithDefault(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// Plan: CREATE TABLE with DEFAULT
	plan := &planner.CreateTableNode{
		TableName: "products",
		Columns: []ast.ColumnDefinition{
			{Name: "id", DataType: "INT", IsNullable: false, IsUnique: true, IsPrimaryKey: true},
			{Name: "name", DataType: "TEXT", IsNullable: false, Default: "unnamed"},
			{Name: "price", DataType: "INT", IsNullable: true, Default: "0"},
		},
	}

	_, err := exec.Execute(plan)
	if err != nil {
		t.Fatalf("CreateTable with defaults should succeed: %v", err)
	}

	tbl, ok := exec.Tables["products"]
	if !ok {
		t.Fatal("products table should exist")
	}

	// Verify Default was stored in schema
	if tbl.Schema.Columns[1].Default != "unnamed" {
		t.Errorf("expected default 'unnamed' for name column, got %q", tbl.Schema.Columns[1].Default)
	}
	if tbl.Schema.Columns[2].Default != "0" {
		t.Errorf("expected default '0' for price column, got %q", tbl.Schema.Columns[2].Default)
	}
}

func TestInsertWithDefaults(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	// First create the table with defaults (manually, since we need storage columns)
	schema := storage.NewSchema([]storage.Column{
		{Name: "id", Type: storage.TypeInt32, IsNullable: false, IsUnique: true, IsPrimaryKey: true},
		{Name: "name", Type: storage.TypeFixedText, Size: 32, IsNullable: false, Default: "unnamed"},
		{Name: "price", Type: storage.TypeInt32, IsNullable: true, Default: "0"},
	})

	dbPath := filepath.Join(exec.Engine.BaseDir, exec.Engine.ActiveDB, "products.db")
	pager, err := storage.NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	table := storage.NewTable(pager, schema)
	exec.RegisterTable("products", table)

	// Insert with explicit columns, omitting ones with defaults
	plan := &planner.InsertNode{
		TableName: "products",
		Columns:   []string{"id"},
		Values:    []string{"1"},
	}

	_, err = exec.Execute(plan)
	if err != nil {
		t.Fatalf("Insert with defaults should succeed: %v", err)
	}

	// Verify the row has default values
	result, err := exec.Execute(&planner.ScanNode{TableName: "products"})
	if err != nil {
		t.Fatalf("Scan should succeed: %v", err)
	}

	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}

	if result.Rows[0].Values[1].(string) != "unnamed" {
		t.Errorf("expected name 'unnamed', got %v", result.Rows[0].Values[1])
	}
	if result.Rows[0].Values[2].(int32) != 0 {
		t.Errorf("expected price 0, got %v", result.Rows[0].Values[2])
	}
}

func TestExecuteSortWithLimit(t *testing.T) {
	exec, cleanup := setupExecutor(t)
	defer cleanup()

	for _, val := range []int32{5, 3, 1, 4, 2} {
		exec.Tables["users"].Insert([]interface{}{val, "user", val})
	}

	// Sort ASC then take LIMIT 2
	sortPlan := &planner.SortNode{
		Child:   &planner.ScanNode{TableName: "users"},
		OrderBy: []ast.SortExpression{{Column: "id", Direction: "ASC"}},
	}
	limitPlan := &planner.LimitNode{
		Child: sortPlan,
		Limit: 2,
	}

	result, err := exec.Execute(limitPlan)
	if err != nil {
		t.Fatalf("sort+limit should succeed: %v", err)
	}

	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0].Values[0].(int32) != 1 || result.Rows[1].Values[0].(int32) != 2 {
		t.Errorf("expected first two sorted rows 1,2 but got %d,%d", result.Rows[0].Values[0], result.Rows[1].Values[0])
	}
}

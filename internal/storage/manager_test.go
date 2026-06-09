package storage

import (
	"path/filepath"
	"testing"
)

func setupTable(t *testing.T) *Table {
	t.Helper()
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false, IsPrimaryKey: true},
		{Name: "name", Type: TypeFixedText, Size: 32, IsNullable: true},
		{Name: "age", Type: TypeInt32, IsNullable: true},
	})

	return NewTable(pager, schema)
}

func TestNewTable(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	if table.Pager == nil {
		t.Error("pager should not be nil")
	}
	if table.Schema == nil {
		t.Error("schema should not be nil")
	}
}

func TestTableInsertAndSelectAll(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	err := table.Insert([]interface{}{int32(1), "Alice", int32(30)})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	err = table.Insert([]interface{}{int32(2), "Bob", int32(25)})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	if rows[0].Values[0].(int32) != 1 || rows[0].Values[1].(string) != "Alice" {
		t.Errorf("row 0 mismatch: %v", rows[0].Values)
	}
	if rows[1].Values[0].(int32) != 2 || rows[1].Values[1].(string) != "Bob" {
		t.Errorf("row 1 mismatch: %v", rows[1].Values)
	}

	// Check physical addresses
	if rows[0].PageID != 0 || rows[1].PageID != 0 {
		t.Errorf("expected both rows on page 0, got %d, %d", rows[0].PageID, rows[1].PageID)
	}
}

func TestTableInsertNullValues(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	err := table.Insert([]interface{}{int32(1), nil, nil})
	if err != nil {
		t.Fatalf("Insert with nulls failed: %v", err)
	}

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0].Values[1] != nil || rows[0].Values[2] != nil {
		t.Errorf("expected nil values, got %v", rows[0].Values)
	}
}

func TestTableInsertNotNullViolation(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	err := table.Insert([]interface{}{nil, "test", int32(1)})
	if err == nil {
		t.Error("expected error for inserting nil into NOT NULL column")
	}
}

func TestTableInsertAndUpdate(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	err := table.Insert([]interface{}{int32(1), "Old Name", int32(20)})
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	// Update the row
	err = table.Update(rows[0].PageID, rows[0].SlotID, []interface{}{int32(1), "New Name", int32(25)})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	// Verify update
	rows, err = table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	if rows[0].Values[1].(string) != "New Name" {
		t.Errorf("expected 'New Name', got %v", rows[0].Values[1])
	}
	if rows[0].Values[2].(int32) != 25 {
		t.Errorf("expected 25, got %v", rows[0].Values[2])
	}
}

func TestTableDelete(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	table.Insert([]interface{}{int32(1), "Alice", int32(30)})
	table.Insert([]interface{}{int32(2), "Bob", int32(25)})

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Delete first row
	err = table.Delete(rows[0].PageID, rows[0].SlotID)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	rows, err = table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(rows))
	}
	if rows[0].Values[1].(string) != "Bob" {
		t.Errorf("expected Bob, got %v", rows[0].Values[1])
	}
}

func TestTableDeleteInvalidSlot(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	err := table.Delete(0, 0)
	if err != nil {
		// Should either succeed (no-op) or fail, but should not panic
	}
}

func TestTableUpdateInvalidSlot(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	err := table.Update(0, 0, []interface{}{int32(1), "test", int32(1)})
	if err == nil {
		// May or may not error, but should not panic
	}
}

func TestTableInsertMultiplePages(t *testing.T) {
	// Create a schema with a very large text field so rows are big
	schema := NewSchema([]Column{
		{Name: "data", Type: TypeFixedText, Size: 4000, IsNullable: false},
	})

	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	pager, _ := NewPager(dbPath)
	table := NewTable(pager, schema)
	defer table.Pager.Close()

	// Insert one row that fills most of page 0
	bigStr := ""
	for i := 0; i < 4000; i++ {
		bigStr += "x"
	}
	err := table.Insert([]interface{}{bigStr})
	if err != nil {
		t.Fatalf("Insert 1 failed: %v", err)
	}

	// Insert another row - should go to a new page
	err = table.Insert([]interface{}{bigStr})
	if err != nil {
		t.Fatalf("Insert 2 failed: %v", err)
	}

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}

	// Each row should be on a different page
	if rows[0].PageID == rows[1].PageID {
		t.Error("expected rows to be on different pages")
	}
}

func TestTableSelectAllEmpty(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll on empty table failed: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("expected 0 rows, got %d", len(rows))
	}
}

func TestTableInsertManyRows(t *testing.T) {
	table := setupTable(t)
	defer table.Pager.Close()

	numRows := 50
	for i := 0; i < numRows; i++ {
		err := table.Insert([]interface{}{int32(i), "user", int32(i * 2)})
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	rows, err := table.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	if len(rows) != numRows {
		t.Errorf("expected %d rows, got %d", numRows, len(rows))
	}

	// Verify data integrity
	for i, row := range rows {
		if row.Values[0].(int32) != int32(i) {
			t.Errorf("row %d: expected id %d, got %v", i, i, row.Values[0])
		}
	}
}

func TestTableInsertAndSelectAfterReopen(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create and write
	pager1, _ := NewPager(dbPath)
	schema1 := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
		{Name: "val", Type: TypeFixedText, Size: 10, IsNullable: true},
	})
	table1 := NewTable(pager1, schema1)
	table1.Insert([]interface{}{int32(1), "hello"})
	table1.Insert([]interface{}{int32(2), "world"})
	pager1.Close()

	// Reopen and read
	pager2, _ := NewPager(dbPath)
	schema2 := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
		{Name: "val", Type: TypeFixedText, Size: 10, IsNullable: true},
	})
	table2 := NewTable(pager2, schema2)

	rows, err := table2.SelectAll()
	if err != nil {
		t.Fatalf("SelectAll after reopen failed: %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after reopen, got %d", len(rows))
	}
	if rows[0].Values[0].(int32) != 1 || rows[0].Values[1].(string) != "hello" {
		t.Errorf("row 0 mismatch: %v", rows[0].Values)
	}
	if rows[1].Values[0].(int32) != 2 || rows[1].Values[1].(string) != "world" {
		t.Errorf("row 1 mismatch: %v", rows[1].Values)
	}
	pager2.Close()
}

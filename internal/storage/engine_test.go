package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewEngine(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)
	if engine.BaseDir != tempDir {
		t.Errorf("expected %s, got %s", tempDir, engine.BaseDir)
	}
	if engine.ActiveDB != "" {
		t.Errorf("expected empty active db, got %s", engine.ActiveDB)
	}
}

func TestCreateDatabase(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)

	err := engine.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	dbPath := filepath.Join(tempDir, "testdb")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("database directory was not created")
	}
}

func TestCreateDatabaseAlreadyExists(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)

	err := engine.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("first CreateDatabase failed: %v", err)
	}

	err = engine.CreateDatabase("testdb")
	if err == nil {
		t.Error("expected error for duplicate database")
	}
}

func TestUseDatabase(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)

	err := engine.CreateDatabase("testdb")
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	err = engine.UseDatabase("testdb")
	if err != nil {
		t.Fatalf("UseDatabase failed: %v", err)
	}

	if engine.ActiveDB != "testdb" {
		t.Errorf("expected testdb, got %s", engine.ActiveDB)
	}
}

func TestUseDatabaseNotExists(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)

	err := engine.UseDatabase("nonexistent")
	if err == nil {
		t.Error("expected error for non-existent database")
	}
}

func TestUseDatabaseChangesActiveDB(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)

	engine.CreateDatabase("db1")
	engine.CreateDatabase("db2")

	engine.UseDatabase("db1")
	if engine.ActiveDB != "db1" {
		t.Errorf("expected db1, got %s", engine.ActiveDB)
	}

	engine.UseDatabase("db2")
	if engine.ActiveDB != "db2" {
		t.Errorf("expected db2, got %s", engine.ActiveDB)
	}
}

func TestNewEngineCreatesBaseDir(t *testing.T) {
	tempDir := filepath.Join(t.TempDir(), "nested", "dir")
	engine := NewEngine(tempDir)
	if engine.BaseDir != tempDir {
		t.Errorf("expected %s, got %s", tempDir, engine.BaseDir)
	}
	if _, err := os.Stat(tempDir); os.IsNotExist(err) {
		t.Errorf("base directory should have been created")
	}
}

func TestCreateDatabaseWithSlash(t *testing.T) {
	tempDir := t.TempDir()
	engine := NewEngine(tempDir)

	err := engine.CreateDatabase("db/name")
	if err == nil {
		t.Error("expected error for database name with slash")
	}
}

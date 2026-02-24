package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestNewPager(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("database file was not created at %s", dbPath)
	}
}

func TestReadWritePage(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	pageID := uint32(0)
	data := make([]byte, PAGE_SIZE)
	copy(data, "hello world")

	err = pager.WritePage(pageID, data)
	if err != nil {
		t.Fatalf("failed to write page: %v", err)
	}

	readData, err := pager.ReadPage(pageID)
	if err != nil {
		t.Fatalf("failed to read page: %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Errorf("read data does not match written data")
	}
}

func TestReadWriteMultiplePages(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	tests := []struct {
		pageID uint32
		data   string
	}{
		{0, "page 0 content"},
		{1, "page 1 content"},
		{5, "page 5 content"},
	}

	for _, tt := range tests {
		data := make([]byte, PAGE_SIZE)
		copy(data, tt.data)

		err = pager.WritePage(tt.pageID, data)
		if err != nil {
			t.Errorf("failed to write page %d: %v", tt.pageID, err)
			continue
		}

		readData, err := pager.ReadPage(tt.pageID)
		if err != nil {
			t.Errorf("failed to read page %d: %v", tt.pageID, err)
			continue
		}

		if !bytes.Equal(data, readData) {
			t.Errorf("page %d: read data does not match written data", tt.pageID)
		}
	}
}

func TestTotalPages(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	if pager.TotalPages() != 0 {
		t.Errorf("expected 0 pages initially, got %d", pager.TotalPages())
	}

	data := make([]byte, PAGE_SIZE)
	copy(data, "test")

	pager.WritePage(0, data)
	if pager.TotalPages() != 1 {
		t.Errorf("expected 1 page after writing to page 0, got %d", pager.TotalPages())
	}

	pager.WritePage(10, data)
	// Writing to page 10 makes the file size (10+1)*PAGE_SIZE
	if pager.TotalPages() != 11 {
		t.Errorf("expected 11 pages after writing to page 10, got %d", pager.TotalPages())
	}
}

func TestReadEmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	// Read from an empty file should return a zeroed page and no error
	data, err := pager.ReadPage(0)
	if err != nil {
		t.Errorf("expected no error when reading from empty file, got %v", err)
	}
	if len(data) != PAGE_SIZE {
		t.Errorf("expected data size %d, got %d", PAGE_SIZE, len(data))
	}
}

func TestNewPagerError(t *testing.T) {
	// Try to open a directory as a file or an invalid path
	_, err := NewPager("")
	if err == nil {
		t.Error("expected error when opening pager with empty filename, got nil")
	}
}

func TestCloseAndOps(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}

	err = pager.Close()
	if err != nil {
		t.Fatalf("failed to close pager: %v", err)
	}

	// Operations after close should fail
	_, err = pager.ReadPage(0)
	if err == nil {
		t.Error("expected error when reading from closed pager, got nil")
	}

	data := make([]byte, PAGE_SIZE)
	err = pager.WritePage(0, data)
	if err == nil {
		t.Error("expected error when writing to closed pager, got nil")
	}

	if pager.TotalPages() != 0 {
		// TotalPages returns 0 on stat error, which is expected after close
	}
}

func TestWritePageSizeValidation(t *testing.T) {
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	pager, err := NewPager(dbPath)
	if err != nil {
		t.Fatalf("failed to create pager: %v", err)
	}
	defer pager.Close()

	tests := []struct {
		name string
		size int
	}{
		{"Too petite", PAGE_SIZE - 1},
		{"Too large", PAGE_SIZE + 1},
		{"Empty", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			invalidData := make([]byte, tt.size)
			err = pager.WritePage(0, invalidData)
			if err == nil {
				t.Errorf("expected error when writing data with invalid size %d, got nil", tt.size)
			}
		})
	}
}

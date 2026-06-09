package storage

import (
	"testing"
)

func TestNewSchema(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: TypeInt32, IsNullable: false, IsPrimaryKey: true},
		{Name: "name", Type: TypeFixedText, Size: 100, IsNullable: true},
	}
	schema := NewSchema(cols)

	if len(schema.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d", len(schema.Columns))
	}
	if schema.Columns[0].Size != 4 {
		t.Errorf("expected int32 size 4, got %d", schema.Columns[0].Size)
	}
	if schema.Columns[1].Size != 100 {
		t.Errorf("expected text size 100, got %d", schema.Columns[1].Size)
	}
}

func TestNewSchemaBitmapSize(t *testing.T) {
	tests := []struct {
		numCols     int
		bitmapSize  uint32
	}{
		{0, 0},
		{1, 1},
		{7, 1},
		{8, 1},
		{9, 2},
		{16, 2},
		{17, 3},
	}

	for _, tt := range tests {
		cols := make([]Column, tt.numCols)
		for i := 0; i < tt.numCols; i++ {
			cols[i] = Column{Name: "c", Type: TypeInt32}
		}
		schema := NewSchema(cols)
		if schema.BitmapSize != tt.bitmapSize {
			t.Errorf("%d cols: expected bitmap size %d, got %d", tt.numCols, tt.bitmapSize, tt.bitmapSize)
		}
	}
}

func TestNewSchemaTotalSize(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: TypeInt32},
		{Name: "b", Type: TypeUint32},
		{Name: "c", Type: TypeFixedText, Size: 50},
	}
	schema := NewSchema(cols)

	// bitmap(1) + int32(4) + uint32(4) + text(50) = 59
	expected := uint32(1 + 4 + 4 + 50)
	if schema.TotalSize != expected {
		t.Errorf("expected total size %d, got %d", expected, schema.TotalSize)
	}
}

func TestGetColumnOffset(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: TypeInt32},
		{Name: "b", Type: TypeFixedText, Size: 20},
		{Name: "c", Type: TypeUint32},
	}
	schema := NewSchema(cols)

	offsets := []struct {
		index    int
		expected uint32
	}{
		{0, 0},              // first col at start (after bitmap)
		{1, 4},              // after int32 (4 bytes)
		{2, 4 + 20},         // after int32 + text(20)
	}

	for _, o := range offsets {
		offset := schema.GetColumnOffset(o.index)
		if offset != o.expected {
			t.Errorf("column %d: expected offset %d, got %d", o.index, o.expected, offset)
		}
	}
}

func TestGetColumnOffsetZero(t *testing.T) {
	cols := []Column{
		{Name: "a", Type: TypeInt32},
	}
	schema := NewSchema(cols)

	offset := schema.GetColumnOffset(0)
	if offset != 0 {
		t.Errorf("expected 0, got %d", offset)
	}
}

func TestNewSchemaEmptyColumns(t *testing.T) {
	schema := NewSchema([]Column{})
	if len(schema.Columns) != 0 {
		t.Errorf("expected 0 columns, got %d", len(schema.Columns))
	}
	if schema.BitmapSize != 0 {
		t.Errorf("expected 0 bitmap size, got %d", schema.BitmapSize)
	}
	if schema.TotalSize != 0 {
		t.Errorf("expected 0 total size, got %d", schema.TotalSize)
	}
}

func TestColumnDefaults(t *testing.T) {
	col := Column{Name: "test"}
	if col.Type != 0 {
		t.Errorf("expected default type 0, got %d", col.Type)
	}
	if col.Size != 0 {
		t.Errorf("expected default size 0, got %d", col.Size)
	}
}

func TestForeignKeyRef(t *testing.T) {
	ref := &ForeignKeyRef{Table: "users", Column: "id"}
	if ref.Table != "users" || ref.Column != "id" {
		t.Errorf("ForeignKeyRef fields mismatch")
	}
}

func TestSchemaWithAllTypes(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: TypeInt32},
		{Name: "uid", Type: TypeUint32},
		{Name: "name", Type: TypeFixedText, Size: 255},
	}
	schema := NewSchema(cols)

	// bitmap(1) + int32(4) + uint32(4) + text(255) = 264
	if schema.TotalSize != 264 {
		t.Errorf("expected 264, got %d", schema.TotalSize)
	}
}

func TestSchemaForcesIntSize(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: TypeInt32, Size: 100}, // should be forced to 4
	}
	schema := NewSchema(cols)
	if schema.Columns[0].Size != 4 {
		t.Errorf("expected int32 size forced to 4, got %d", schema.Columns[0].Size)
	}
}

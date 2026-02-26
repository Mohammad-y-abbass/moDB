package storage

type DataType uint8

const (
	TypeInt32     DataType = iota // 4 bytes
	TypeUint32                    // 4 bytes
	TypeFixedText                 // We'll define a fixed size, e.g., 32 bytes
)

// ForeignKeyRef stores the target table and column for a FOREIGN KEY constraint.
type ForeignKeyRef struct {
	Table  string `json:"table"`
	Column string `json:"column"`
}

type Column struct {
	Name         string         `json:"name"`
	Type         DataType       `json:"type"`
	Size         uint32         `json:"size"`
	IsNullable   bool           `json:"is_nullable"`
	IsUnique     bool           `json:"is_unique"`
	IsPrimaryKey bool           `json:"is_primary_key"`
	References   *ForeignKeyRef `json:"references,omitempty"`
}

type Schema struct {
	Columns    []Column `json:"columns"`
	TotalSize  uint32   `json:"total_size"`
	BitmapSize uint32   `json:"bitmap_size"`
}

func NewSchema(cols []Column) *Schema {
	var total uint32
	// Calculate bitmap size (1 bit per column, rounded up to bytes)
	bitmapSize := uint32((len(cols) + 7) / 8)
	total = bitmapSize

	for i := range cols {
		// Ensure size is set correctly for fixed types
		if cols[i].Type == TypeInt32 || cols[i].Type == TypeUint32 {
			cols[i].Size = 4
		}
		total += cols[i].Size
	}
	return &Schema{
		Columns:    cols,
		TotalSize:  total,
		BitmapSize: bitmapSize,
	}
}

// GetColumnOffset returns how many bytes to skip to reach a specific column
func (s *Schema) GetColumnOffset(colIndex int) uint32 {
	var offset uint32
	for i := 0; i < colIndex; i++ {
		offset += s.Columns[i].Size
	}
	return offset
}

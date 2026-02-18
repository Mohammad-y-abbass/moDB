package storage

type DataType uint8

const (
	TypeInt32     DataType = iota // 4 bytes
	TypeUint32                    // 4 bytes
	TypeFixedText                 // We'll define a fixed size, e.g., 32 bytes
)

type Column struct {
	Name string
	Type DataType
	Size uint32 // Physical size in bytes
}

type Schema struct {
	Columns   []Column
	TotalSize uint32 // Sum of all column sizes
}

func NewSchema(cols []Column) *Schema {
	var total uint32
	for i := range cols {
		// Ensure size is set correctly for fixed types
		if cols[i].Type == TypeInt32 || cols[i].Type == TypeUint32 {
			cols[i].Size = 4
		}
		total += cols[i].Size
	}
	return &Schema{
		Columns:   cols,
		TotalSize: total,
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

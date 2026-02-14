package storage

import (
	"encoding/binary"
	"fmt"
)

type DataType byte

const (
	TypeInt    DataType = 1
	TypeString DataType = 2
)

type Column struct {
	Name string
	Type DataType
}

type Schema struct {
	Columns []Column
}

// Serialize turns the Schema into a 4096-byte page
func (s *Schema) Serialize() []byte {
	page := make([]byte, PAGE_SIZE)

	// 1. Write Magic Number "MYDB"
	binary.BigEndian.PutUint32(page[0:4], 0x4D594442)

	// 2. Write Version 1
	binary.BigEndian.PutUint16(page[4:6], 1)

	// 3. Write Column Count
	binary.BigEndian.PutUint16(page[6:8], uint16(len(s.Columns)))

	// 4. Write Columns
	curr := 8
	for _, col := range s.Columns {
		page[curr] = byte(col.Type)
		curr++

		nameBytes := []byte(col.Name)
		page[curr] = byte(len(nameBytes))
		curr++

		copy(page[curr:], nameBytes)
		curr += len(nameBytes)
	}

	return page
}

// Deserialize reads the Schema back from raw bytes
func DeserializeSchema(page []byte) (*Schema, error) {
	magic := binary.BigEndian.Uint32(page[0:4])
	if magic != 0x4D594442 {
		return nil, fmt.Errorf("invalid magic number: %x", magic)
	}

	colCount := binary.BigEndian.Uint16(page[6:8])
	columns := make([]Column, colCount)

	curr := 8
	for i := 0; i < int(colCount); i++ {
		colType := DataType(page[curr])
		curr++

		nameLen := int(page[curr])
		curr++

		name := string(page[curr : curr+nameLen])
		curr += nameLen

		columns[i] = Column{Name: name, Type: colType}
	}

	return &Schema{Columns: columns}, nil
}

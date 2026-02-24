package storage

import (
	"encoding/binary"
	"fmt"
)

// Row represents a single record in memory before/after serialization
type Row struct {
	Values []interface{} // Can hold int32, uint32, or string
	PageID uint32
	SlotID uint16
}

// Serialize converts Go values into a fixed-length byte slice based on the Schema
func (s *Schema) Serialize(row Row) ([]byte, error) {
	if len(row.Values) != len(s.Columns) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(s.Columns), len(row.Values))
	}

	data := make([]byte, s.TotalSize)
	bitmap := make([]byte, s.BitmapSize)
	currentOffset := s.BitmapSize

	for i, col := range s.Columns {
		val := row.Values[i]

		if val == nil {
			if !col.IsNullable {
				return nil, fmt.Errorf("column %s is NOT NULL but got nil", col.Name)
			}
			// Set the bit in the null bitmap
			bitmap[i/8] |= (1 << (7 - (i % 8)))
			currentOffset += col.Size
			continue
		}

		switch col.Type {
		case TypeInt32:
			v, ok := val.(int32)
			if !ok {
				return nil, fmt.Errorf("column %s expects int32", col.Name)
			}
			binary.LittleEndian.PutUint32(data[currentOffset:currentOffset+4], uint32(v))

		case TypeUint32:
			v, ok := val.(uint32)
			if !ok {
				return nil, fmt.Errorf("column %s expects uint32", col.Name)
			}
			binary.LittleEndian.PutUint32(data[currentOffset:currentOffset+4], v)

		case TypeFixedText:
			v, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("column %s expects string", col.Name)
			}
			copy(data[currentOffset:currentOffset+col.Size], v)
		}

		currentOffset += col.Size
	}

	// Prepend the bitmap to the data
	copy(data[:s.BitmapSize], bitmap)

	return data, nil
}

// Deserialize converts a raw byte slice back into a Row struct
func (s *Schema) Deserialize(data []byte) (Row, error) {
	if uint32(len(data)) < s.TotalSize {
		return Row{}, fmt.Errorf("data too short for schema")
	}

	bitmap := data[:s.BitmapSize]
	values := make([]interface{}, len(s.Columns))
	currentOffset := s.BitmapSize

	for i, col := range s.Columns {
		// Check if null bit is set
		isNull := (bitmap[i/8] & (1 << (7 - (i % 8)))) != 0
		if isNull {
			values[i] = nil
			currentOffset += col.Size
			continue
		}

		switch col.Type {
		case TypeInt32:
			v := binary.LittleEndian.Uint32(data[currentOffset : currentOffset+4])
			values[i] = int32(v)

		case TypeUint32:
			v := binary.LittleEndian.Uint32(data[currentOffset : currentOffset+4])
			values[i] = v

		case TypeFixedText:
			rawStr := data[currentOffset : currentOffset+col.Size]
			end := 0
			for end < len(rawStr) && rawStr[end] != 0 {
				end++
			}
			values[i] = string(rawStr[:end])
		}

		currentOffset += col.Size
	}

	return Row{Values: values}, nil
}

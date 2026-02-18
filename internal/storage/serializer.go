package storage

import (
	"encoding/binary"
	"fmt"
)

// Row represents a single record in memory before/after serialization
type Row struct {
	Values []interface{} // Can hold int32, uint32, or string
}

// Serialize converts Go values into a fixed-length byte slice based on the Schema
func (s *Schema) Serialize(row Row) ([]byte, error) {
	if len(row.Values) != len(s.Columns) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(s.Columns), len(row.Values))
	}

	data := make([]byte, s.TotalSize)
	var currentOffset uint32 = 0

	for i, col := range s.Columns {
		val := row.Values[i]

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
			// Copy string bytes into the fixed-size slot
			// If string is shorter than Size, the rest remains zeros (padding)
			// If string is longer, it is truncated
			copy(data[currentOffset:currentOffset+col.Size], v)
		}

		currentOffset += col.Size
	}

	return data, nil
}

// Deserialize converts a raw byte slice back into a Row struct
func (s *Schema) Deserialize(data []byte) (Row, error) {
	if uint32(len(data)) < s.TotalSize {
		return Row{}, fmt.Errorf("data too short for schema")
	}

	values := make([]interface{}, len(s.Columns))
	var currentOffset uint32 = 0

	for i, col := range s.Columns {
		switch col.Type {
		case TypeInt32:
			v := binary.LittleEndian.Uint32(data[currentOffset : currentOffset+4])
			values[i] = int32(v)

		case TypeUint32:
			v := binary.LittleEndian.Uint32(data[currentOffset : currentOffset+4])
			values[i] = v

		case TypeFixedText:
			// Read the fixed size and trim any trailing null bytes (zeros)
			// This makes it act like a normal Go string
			rawStr := data[currentOffset : currentOffset+col.Size]
			// Find the first null byte to trim padding
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

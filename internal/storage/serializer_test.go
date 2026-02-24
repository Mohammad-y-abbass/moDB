package storage

import (
	"testing"
)

func TestSerializationWithNulls(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
		{Name: "name", Type: TypeFixedText, Size: 10, IsNullable: true},
		{Name: "age", Type: TypeInt32, IsNullable: true},
	})

	// Case 1: All values present
	row1 := Row{Values: []interface{}{int32(1), "mohammad", int32(25)}}
	data1, err := schema.Serialize(row1)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	row1_back, err := schema.Deserialize(data1)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if row1_back.Values[0].(int32) != 1 || row1_back.Values[1].(string) != "mohammad" || row1_back.Values[2].(int32) != 25 {
		t.Errorf("Mismatch in row1: %v", row1_back.Values)
	}

	// Case 2: Null values
	row2 := Row{Values: []interface{}{int32(2), nil, nil}}
	data2, err := schema.Serialize(row2)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	row2_back, err := schema.Deserialize(data2)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if row2_back.Values[0].(int32) != 2 || row2_back.Values[1] != nil || row2_back.Values[2] != nil {
		t.Errorf("Mismatch in row2 (nulls): %v", row2_back.Values)
	}

	// Case 3: Error on NOT NULL column
	row3 := Row{Values: []interface{}{nil, "test", int32(1)}}
	_, err = schema.Serialize(row3)
	if err == nil {
		t.Error("Expected error when serializing nil to NOT NULL column")
	}
}

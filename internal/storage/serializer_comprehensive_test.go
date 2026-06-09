package storage

import (
	"testing"
)

func TestSerializeDeserializeInt32(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
	})

	row := Row{Values: []interface{}{int32(42)}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(int32) != 42 {
		t.Errorf("expected 42, got %v", back.Values[0])
	}
}

func TestSerializeDeserializeUint32(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "uid", Type: TypeUint32, IsNullable: false},
	})

	row := Row{Values: []interface{}{uint32(100)}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(uint32) != 100 {
		t.Errorf("expected 100, got %v", back.Values[0])
	}
}

func TestSerializeDeserializeFixedText(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "name", Type: TypeFixedText, Size: 32, IsNullable: false},
	})

	row := Row{Values: []interface{}{"hello"}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(string) != "hello" {
		t.Errorf("expected hello, got %v", back.Values[0])
	}
}

func TestSerializeDeserializeNullValues(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
		{Name: "name", Type: TypeFixedText, Size: 10, IsNullable: true},
		{Name: "age", Type: TypeInt32, IsNullable: true},
	})

	// All nulls except not-null
	row := Row{Values: []interface{}{int32(1), nil, nil}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(int32) != 1 {
		t.Errorf("expected 1, got %v", back.Values[0])
	}
	if back.Values[1] != nil {
		t.Errorf("expected nil, got %v", back.Values[1])
	}
	if back.Values[2] != nil {
		t.Errorf("expected nil, got %v", back.Values[2])
	}
}

func TestSerializeNullNotNullColumn(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
	})

	row := Row{Values: []interface{}{nil}}
	_, err := schema.Serialize(row)
	if err == nil {
		t.Error("expected error when serializing nil to NOT NULL column")
	}
}

func TestSerializeTypeMismatchInt32(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
	})

	_, err := schema.Serialize(Row{Values: []interface{}{"not an int"}})
	if err == nil {
		t.Error("expected error for type mismatch on int32")
	}
}

func TestSerializeTypeMismatchUint32(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "uid", Type: TypeUint32, IsNullable: false},
	})

	_, err := schema.Serialize(Row{Values: []interface{}{"not a uint"}})
	if err == nil {
		t.Error("expected error for type mismatch on uint32")
	}
}

func TestSerializeTypeMismatchText(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "name", Type: TypeFixedText, Size: 10, IsNullable: false},
	})

	_, err := schema.Serialize(Row{Values: []interface{}{int32(123)}})
	if err == nil {
		t.Error("expected error for type mismatch on text")
	}
}

func TestSerializeValueCountMismatch(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "a", Type: TypeInt32},
		{Name: "b", Type: TypeInt32},
	})

	_, err := schema.Serialize(Row{Values: []interface{}{int32(1)}})
	if err == nil {
		t.Error("expected error for value count mismatch")
	}
}

func TestDeserializeDataTooShort(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "a", Type: TypeInt32},
	})

	_, err := schema.Deserialize([]byte{0x00})
	if err == nil {
		t.Error("expected error for data too short")
	}
}

func TestSerializeDeserializeAllTypes(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "id", Type: TypeInt32, IsNullable: false},
		{Name: "uid", Type: TypeUint32, IsNullable: true},
		{Name: "name", Type: TypeFixedText, Size: 32, IsNullable: true},
	})

	row := Row{Values: []interface{}{int32(-100), uint32(200), "test"}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(int32) != -100 {
		t.Errorf("expected -100, got %v", back.Values[0])
	}
	if back.Values[1].(uint32) != 200 {
		t.Errorf("expected 200, got %v", back.Values[1])
	}
	if back.Values[2].(string) != "test" {
		t.Errorf("expected test, got %v", back.Values[2])
	}
}

func TestSerializeDeserializeMixedNulls(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "a", Type: TypeInt32, IsNullable: true},
		{Name: "b", Type: TypeInt32, IsNullable: false},
		{Name: "c", Type: TypeFixedText, Size: 10, IsNullable: true},
		{Name: "d", Type: TypeFixedText, Size: 10, IsNullable: false},
	})

	row := Row{Values: []interface{}{nil, int32(1), nil, "present"}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0] != nil {
		t.Errorf("expected nil, got %v", back.Values[0])
	}
	if back.Values[1].(int32) != 1 {
		t.Errorf("expected 1, got %v", back.Values[1])
	}
	if back.Values[2] != nil {
		t.Errorf("expected nil, got %v", back.Values[2])
	}
	if back.Values[3].(string) != "present" {
		t.Errorf("expected present, got %v", back.Values[3])
	}
}

func TestSerializeDeserializeEmptyText(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "name", Type: TypeFixedText, Size: 32, IsNullable: false},
	})

	row := Row{Values: []interface{}{""}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(string) != "" {
		t.Errorf("expected empty string, got %q", back.Values[0])
	}
}

func TestSerializeDeserializeMaxText(t *testing.T) {
	textLen := 100
	schema := NewSchema([]Column{
		{Name: "content", Type: TypeFixedText, Size: uint32(textLen), IsNullable: false},
	})

	longStr := ""
	for i := 0; i < textLen; i++ {
		longStr += "a"
	}

	// Text should be truncated to fit the fixed size
	row := Row{Values: []interface{}{longStr}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Deserialize will trim null bytes
	if len(back.Values[0].(string)) > textLen {
		t.Errorf("expected at most %d chars, got %d", textLen, len(back.Values[0].(string)))
	}
}

func TestSerializeDeserializeNullBitmapEdgeCases(t *testing.T) {
	// Test with 9 columns to ensure bitmap crosses byte boundary
	cols := make([]Column, 9)
	for i := 0; i < 9; i++ {
		cols[i] = Column{
			Name:       string(rune('A' + i)),
			Type:       TypeInt32,
			IsNullable: true,
		}
	}
	schema := NewSchema(cols)

	values := make([]interface{}, 9)
	for i := 0; i < 9; i++ {
		if i%2 == 0 {
			values[i] = nil // Even columns are null
		} else {
			values[i] = int32(i * 10)
		}
	}

	row := Row{Values: values}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	for i := 0; i < 9; i++ {
		if i%2 == 0 {
			if back.Values[i] != nil {
				t.Errorf("col %d: expected nil, got %v", i, back.Values[i])
			}
		} else {
			expected := int32(i * 10)
			if back.Values[i].(int32) != expected {
				t.Errorf("col %d: expected %d, got %v", i, expected, back.Values[i])
			}
		}
	}
}

func TestRowStruct(t *testing.T) {
	row := Row{
		Values: []interface{}{int32(1), "test"},
		PageID: 5,
		SlotID: 3,
	}
	if row.PageID != 5 || row.SlotID != 3 {
		t.Errorf("Row fields mismatch")
	}
}

func TestSerializeNegativeInt32(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "val", Type: TypeInt32, IsNullable: false},
	})

	row := Row{Values: []interface{}{int32(-2147483648)}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(int32) != -2147483648 {
		t.Errorf("expected min int32, got %v", back.Values[0])
	}
}

func TestSerializeMaxUint32(t *testing.T) {
	schema := NewSchema([]Column{
		{Name: "val", Type: TypeUint32, IsNullable: false},
	})

	row := Row{Values: []interface{}{uint32(4294967295)}}
	data, err := schema.Serialize(row)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	back, err := schema.Deserialize(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if back.Values[0].(uint32) != 4294967295 {
		t.Errorf("expected max uint32, got %v", back.Values[0])
	}
}

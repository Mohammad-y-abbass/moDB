package storage

import (
	"testing"
)

func makePage() *SlottedPage {
	data := make([]byte, PAGE_SIZE)
	page := NewSlottedPage(data)
	page.InitHeader()
	return page
}

func TestNewSlottedPage(t *testing.T) {
	data := make([]byte, PAGE_SIZE)
	page := NewSlottedPage(data)
	if page.data == nil {
		t.Error("data should not be nil")
	}
	if len(page.data) != PAGE_SIZE {
		t.Errorf("expected size %d, got %d", PAGE_SIZE, len(page.data))
	}
}

func TestInitHeader(t *testing.T) {
	page := makePage()
	// After InitHeader: 0 slots, free ptr = PAGE_SIZE
	if page.data[0] != 0 || page.data[1] != 0 {
		t.Errorf("expected 0 slots, got %d", page.data[0])
	}
	// Free space pointer at bytes 2-3 (little endian)
	freePtr := uint16(page.data[2]) | uint16(page.data[3])<<8
	if freePtr != PAGE_SIZE {
		t.Errorf("expected free pointer %d, got %d", PAGE_SIZE, freePtr)
	}
}

func TestInsert(t *testing.T) {
	page := makePage()
	rowData := []byte("hello world")

	slotID, err := page.Insert(rowData)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if slotID != 0 {
		t.Errorf("expected slot 0, got %d", slotID)
	}

	// Verify the row data
	retrieved := page.GetRow(slotID)
	if string(retrieved) != string(rowData) {
		t.Errorf("expected %q, got %q", string(rowData), string(retrieved))
	}
}

func TestInsertMultiple(t *testing.T) {
	page := makePage()
	rows := [][]byte{
		[]byte("row1"),
		[]byte("row2_longer"),
		[]byte("row3"),
	}

	for i, row := range rows {
		slotID, err := page.Insert(row)
		if err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
		if slotID != uint16(i) {
			t.Errorf("expected slot %d, got %d", i, slotID)
		}
	}

	for i, row := range rows {
		retrieved := page.GetRow(uint16(i))
		if string(retrieved) != string(row) {
			t.Errorf("row %d: expected %q, got %q", i, string(row), string(retrieved))
		}
	}
}

func TestInsertPageFull(t *testing.T) {
	page := makePage()
	// Create a row that fills the page
	rowData := make([]byte, PAGE_SIZE-HeaderSize-SlotSize) // max possible
	_, err := page.Insert(rowData)
	if err != nil {
		t.Fatalf("Insert of max row failed: %v", err)
	}

	// This insert should fail - no more room for slot entry even
	_, err = page.Insert([]byte("too much"))
	if err == nil {
		t.Error("expected error for inserting into full page")
	}
}

func TestInsertAfterFullPage(t *testing.T) {
	page := makePage()
	rowData := make([]byte, PAGE_SIZE-HeaderSize-SlotSize-1) // leave 1 byte
	_, err := page.Insert(rowData)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Try inserting minimum data - should still fail because can't fit slot entry
	_, err = page.Insert([]byte{1})
	if err == nil {
		t.Error("expected error when page cannot fit another slot entry")
	}
}

func TestGetRowInvalidSlot(t *testing.T) {
	page := makePage()
	retrieved := page.GetRow(0)
	if retrieved != nil {
		t.Errorf("expected nil for invalid slot, got %v", retrieved)
	}
}

func TestGetRowAfterInsert(t *testing.T) {
	page := makePage()
	rowData := []byte("test data")
	page.Insert(rowData)

	// Request slot beyond numSlots
	retrieved := page.GetRow(999)
	if retrieved != nil {
		t.Errorf("expected nil for out-of-range slot")
	}
}

func TestUpdate(t *testing.T) {
	page := makePage()
	page.Insert([]byte("original"))

	newData := []byte("updated!")
	err := page.Update(0, newData)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}

	retrieved := page.GetRow(0)
	if string(retrieved) != string(newData) {
		t.Errorf("expected %q, got %q", string(newData), string(retrieved))
	}
}

func TestUpdateInvalidSlot(t *testing.T) {
	page := makePage()
	err := page.Update(0, []byte("data"))
	if err == nil {
		t.Error("expected error for updating non-existent slot")
	}
}

func TestUpdateSizeMismatch(t *testing.T) {
	page := makePage()
	_, err := page.Insert([]byte("12345"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Try to update with different size data
	err = page.Update(0, []byte("new data that is longer"))
	if err == nil {
		t.Error("expected error for size mismatch on update")
	}
}

func TestDelete(t *testing.T) {
	page := makePage()
	page.Insert([]byte("delete me"))

	err := page.Delete(0)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	retrieved := page.GetRow(0)
	// Deleted slots have their entry zeroed, so reading returns empty
	// (offset=0, length=0, so offset:offset+length is data[0:0])
	if len(retrieved) != 0 {
		t.Errorf("expected empty data after delete, got %v", retrieved)
	}
}

func TestDeleteInvalidSlot(t *testing.T) {
	page := makePage()
	err := page.Delete(0)
	if err == nil {
		t.Error("expected error for deleting non-existent slot")
	}
}

func TestDeleteMultipleSlots(t *testing.T) {
	page := makePage()
	for i := 0; i < 5; i++ {
		page.Insert([]byte{byte('A' + i)})
	}

	// Delete every other slot
	page.Delete(1)
	page.Delete(3)

	// Slot 0 and 2 should still be readable, 1 and 3 should be empty
	if len(page.GetRow(0)) == 0 {
		t.Error("slot 0 should still contain data")
	}
	if len(page.GetRow(1)) != 0 {
		t.Error("slot 1 should be empty after delete")
	}
	if len(page.GetRow(2)) == 0 {
		t.Error("slot 2 should still contain data")
	}
	if len(page.GetRow(3)) != 0 {
		t.Error("slot 3 should be empty after delete")
	}
}

func TestInsertAfterDelete(t *testing.T) {
	// Deleting slots doesn't reclaim space, but the slot entries are zeroed.
	// New inserts go at the end. Verify this works.
	page := makePage()
	page.Insert([]byte("row0"))
	page.Insert([]byte("row1"))
	page.Insert([]byte("row2"))

	page.Delete(1)

	// Insert another row - should go to slot 3 (not reuse slot 1)
	slotID, err := page.Insert([]byte("row3"))
	if err != nil {
		t.Fatalf("Insert after delete failed: %v", err)
	}
	if slotID != 3 {
		t.Errorf("expected slot 3 for new insert, got %d", slotID)
	}
}

func TestInsertZeroLengthRow(t *testing.T) {
	page := makePage()
	_, err := page.Insert([]byte{})
	if err != nil {
		t.Fatalf("Insert of empty row failed: %v", err)
	}
	retrieved := page.GetRow(0)
	if len(retrieved) != 0 {
		t.Errorf("expected empty row, got %v", retrieved)
	}
}

func TestLargeNumberOfInserts(t *testing.T) {
	page := makePage()
	smallRow := []byte("A")
	count := 0
	for {
		_, err := page.Insert(smallRow)
		if err != nil {
			break
		}
		count++
	}
	// Should have inserted many small rows
	if count < 100 {
		t.Errorf("expected at least 100 small rows, got %d", count)
	}
}

func TestReadAfterMultipleOperations(t *testing.T) {
	page := makePage()

	// Insert
	page.Insert([]byte("first"))
	page.Insert([]byte("second"))
	page.Insert([]byte("third"))

	// Update second
	page.Update(1, []byte("SECOND"))

	// Delete first
	page.Delete(0)

	// Verify
	if len(page.GetRow(0)) != 0 {
		t.Error("slot 0 should be deleted")
	}
	if string(page.GetRow(1)) != "SECOND" {
		t.Errorf("slot 1 should be SECOND, got %q", string(page.GetRow(1)))
	}
	if string(page.GetRow(2)) != "third" {
		t.Errorf("slot 2 should be third, got %q", string(page.GetRow(2)))
	}
}

func TestHeaderValuesAfterInsert(t *testing.T) {
	page := makePage()
	rowData := []byte("test")

	page.Insert(rowData)

	// Slot count should be 1
	slotCount := uint16(page.data[0]) | uint16(page.data[1])<<8
	if slotCount != 1 {
		t.Errorf("expected 1 slot, got %d", slotCount)
	}

	// Free pointer should be less than PAGE_SIZE
	freePtr := uint16(page.data[2]) | uint16(page.data[3])<<8
	if freePtr >= PAGE_SIZE {
		t.Errorf("free pointer should be less than %d, got %d", PAGE_SIZE, freePtr)
	}
}

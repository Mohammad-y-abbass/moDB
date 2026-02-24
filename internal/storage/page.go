package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// Header: 2 bytes for Slot Count, 2 bytes for Free Space Pointer
	HeaderSize = 4
	// Slot Entry: 2 bytes for Offset, 2 bytes for Length
	SlotSize = 4
)

type SlottedPage struct {
	data []byte // The raw 4096 bytes from the Pager
}

func NewSlottedPage(data []byte) *SlottedPage {
	return &SlottedPage{data: data}
}

// InitHeader sets up the metadata for a brand-new page
func (p *SlottedPage) InitHeader() {
	// 0 slots initially
	binary.LittleEndian.PutUint16(p.data[0:2], 0)
	// Free space pointer starts at the very end of the page (4096)
	binary.LittleEndian.PutUint16(p.data[2:4], uint16(PAGE_SIZE))
}

// Insert writes a row and returns its Slot ID
func (p *SlottedPage) Insert(rowData []byte) (uint16, error) {
	numSlots := binary.LittleEndian.Uint16(p.data[0:2])
	freePtr := binary.LittleEndian.Uint16(p.data[2:4])

	// Calculate where the directory ends
	dirEnd := HeaderSize + (numSlots * SlotSize)

	// Check if we have enough room in the "Gap"
	if uint16(len(rowData)+SlotSize) > (freePtr - dirEnd) {
		return 0, errors.New("page full")
	}

	// 1. Calculate the starting address for the new row (growing backwards)
	newOffset := freePtr - uint16(len(rowData))

	// 2. Physical Copy: Write the row bytes into the data heap
	copy(p.data[newOffset:], rowData)

	// 3. Update Directory: Record the address and size of this new row
	slotEntryPos := dirEnd
	binary.LittleEndian.PutUint16(p.data[slotEntryPos:slotEntryPos+2], newOffset)
	binary.LittleEndian.PutUint16(p.data[slotEntryPos+2:slotEntryPos+4], uint16(len(rowData)))

	// 4. Update Header: Increment count and update the Free Space Pointer
	binary.LittleEndian.PutUint16(p.data[0:2], numSlots+1)
	binary.LittleEndian.PutUint16(p.data[2:4], newOffset)

	return numSlots, nil
}

// GetRow retrieves the bytes for a specific slot ID in O(1) time
func (p *SlottedPage) GetRow(slotID uint16) []byte {
	numSlots := binary.LittleEndian.Uint16(p.data[0:2])
	if slotID >= numSlots {
		return nil
	}

	// Jump directly to the slot entry
	slotEntryPos := HeaderSize + (slotID * SlotSize)
	offset := binary.LittleEndian.Uint16(p.data[slotEntryPos : slotEntryPos+2])
	length := binary.LittleEndian.Uint16(p.data[slotEntryPos+2 : slotEntryPos+4])

	return p.data[offset : offset+length]
}

// Update overwrites an existing slot with new row data.
// Since we have fixed-length rows, it's a simple copy.
func (p *SlottedPage) Update(slotID uint16, rowData []byte) error {
	numSlots := binary.LittleEndian.Uint16(p.data[0:2])
	if slotID >= numSlots {
		return errors.New("invalid slot ID")
	}

	slotEntryPos := HeaderSize + (slotID * SlotSize)
	offset := binary.LittleEndian.Uint16(p.data[slotEntryPos : slotEntryPos+2])
	length := binary.LittleEndian.Uint16(p.data[slotEntryPos+2 : slotEntryPos+4])

	if uint16(len(rowData)) != length {
		return fmt.Errorf("row size mismatch: expected %d, got %d", length, len(rowData))
	}

	copy(p.data[offset:offset+length], rowData)
	return nil
}

// Delete marks a slot as deleted by setting its length and offset to 0.
// Real compacting would be better, but this is simple for now.
func (p *SlottedPage) Delete(slotID uint16) error {
	numSlots := binary.LittleEndian.Uint16(p.data[0:2])
	if slotID >= numSlots {
		return errors.New("invalid slot ID")
	}

	slotEntryPos := HeaderSize + (slotID * SlotSize)
	// Zero out the slot entry
	binary.LittleEndian.PutUint16(p.data[slotEntryPos:slotEntryPos+2], 0)
	binary.LittleEndian.PutUint16(p.data[slotEntryPos+2:slotEntryPos+4], 0)
	return nil
}

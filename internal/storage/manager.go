package storage

import (
	"encoding/binary"
	"fmt"
)

type Table struct {
	Pager  *Pager
	Schema *Schema
}

func NewTable(pager *Pager, schema *Schema) *Table {
	return &Table{
		Pager:  pager,
		Schema: schema,
	}
}

// Insert handles the end-to-to workflow: Serialize -> Find Page -> Write
func (t *Table) Insert(values []interface{}) error {
	// 1. Convert Go values to the fixed-length byte format
	row := Row{Values: values}
	rowData, err := t.Schema.Serialize(row)
	if err != nil {
		return err
	}

	// 2. Find a page with enough space
	// For now, we'll just try the very last page in the file
	totalPages := t.Pager.TotalPages()
	var targetPageID uint32

	if totalPages == 0 {
		targetPageID = 0
	} else {
		targetPageID = totalPages - 1
	}

	// 3. Load the page and wrap it in our SlottedPage logic
	pageData, err := t.Pager.ReadPage(targetPageID)
	if err != nil {
		return err
	}
	page := NewSlottedPage(pageData)

	// If it's a brand new page (all zeros), initialize the header
	if totalPages == 0 {
		page.InitHeader()
	}

	// 4. Try to insert into this page
	_, err = page.Insert(rowData)
	if err != nil {
		// If page is full, create a NEW page
		targetPageID = totalPages
		newPageData := make([]byte, PAGE_SIZE)
		page = NewSlottedPage(newPageData)
		page.InitHeader()

		_, err = page.Insert(rowData)
		if err != nil {
			return fmt.Errorf("failed to insert even into new page: %w", err)
		}
	}

	// 5. Commit the changes back to the physical disk
	return t.Pager.WritePage(targetPageID, page.data)
}

// SelectAll is a "Full Table Scan" - the simplest way to read data
func (t *Table) SelectAll() ([]Row, error) {
	var results []Row
	totalPages := t.Pager.TotalPages()

	for i := uint32(0); i < totalPages; i++ {
		pageData, err := t.Pager.ReadPage(i)
		if err != nil {
			return nil, err
		}

		page := NewSlottedPage(pageData)
		numSlots := uint16(binary.LittleEndian.Uint16(page.data[0:2]))

		for slotID := uint16(0); slotID < numSlots; slotID++ {
			rowData := page.GetRow(slotID)
			row, err := t.Schema.Deserialize(rowData)
			if err != nil {
				return nil, err
			}
			results = append(results, row)
		}
	}

	return results, nil
}

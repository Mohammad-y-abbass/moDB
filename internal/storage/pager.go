package storage

import (
	"fmt"
	"os"
)

const (
	PAGE_SIZE = 4096
)

type Pager struct {
	file *os.File
}

func NewPager(fileName string) (*Pager, error) {
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	return &Pager{file: file}, nil

}

func (p *Pager) ReadPage(pageID uint32) ([]byte, error) {
	data := make([]byte, PAGE_SIZE)
	offset := int64(pageID) * int64(PAGE_SIZE)

	n, err := p.file.ReadAt(data, offset)

	if err != nil {
		if err.Error() == "EOF" {
			return data, nil
		}
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	if n < PAGE_SIZE {
		// Possibly hitting EOF but not getting EOF error, or partial read
		return data, nil
	}

	return data, nil
}

func (p *Pager) WritePage(pageID uint32, data []byte) error {
	if len(data) != PAGE_SIZE {
		return fmt.Errorf("data size %d does not match PageSize %d", len(data), PAGE_SIZE)
	}

	offset := int64(pageID) * int64(PAGE_SIZE)
	_, err := p.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageID, err)
	}

	return nil
}

func (p *Pager) TotalPages() uint32 {
	info, err := p.file.Stat()
	if err != nil {
		return 0
	}
	return uint32(info.Size() / int64(PAGE_SIZE))
}

// Sync ensures data is physically written to the disk hardware
func (p *Pager) Sync() error {
	return p.file.Sync()
}

func (p *Pager) Close() error {
	// ensure no data loss when closing the file
	p.Sync()
	return p.file.Close()
}

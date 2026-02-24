package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

type Engine struct {
	BaseDir  string
	ActiveDB string
}

func NewEngine(baseDir string) *Engine {
	os.MkdirAll(baseDir, 0755)
	return &Engine{BaseDir: baseDir}
}

func (e *Engine) CreateDatabase(name string) error {
	path := filepath.Join(e.BaseDir, name)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		return fmt.Errorf("database already exists: %s", name)
	}
	return os.Mkdir(path, 0755)
}

func (e *Engine) UseDatabase(name string) error {
	path := filepath.Join(e.BaseDir, name)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("database does not exist: %s", name)
	}
	e.ActiveDB = name
	return nil
}

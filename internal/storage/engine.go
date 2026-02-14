package storage

import (
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
	return os.MkdirAll(path, 0755)
}

func (e *Engine) UseDatabase(name string) {
	e.ActiveDB = name
}

package store

import (
	"os"
	"path/filepath"
)

// Repository stores a single download or seller workspace. Missing entries
// must match fs.ErrNotExist. Save and SaveExact must publish atomically.
type Repository interface {
	SaveExact(name string, data []byte) error
	Save(name string, data []byte) error
	Load(name string) ([]byte, error)
	Exists(name string) (bool, error)
	List(prefix string) ([]string, error)
	Remove(name string) error
	Rename(oldName, newName string) error
	Append(name string, data []byte) error
}

// Append adds one complete record. The caller serializes writers to a journal.
func (s *Store) Append(name string, data []byte) error {
	path, err := s.path(name)
	if err != nil {
		return err
	}
	return appendFile(path, data)
}

func appendFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(data); err != nil {
		return err
	}
	return file.Close()
}

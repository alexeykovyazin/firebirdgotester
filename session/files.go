package session

import "os"

func createFile(path string) (*os.File, error) {
	return os.Create(path)
}

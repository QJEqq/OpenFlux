//go:build !unix

package transport

import (
	"errors"
	"os"
)

// Without flock the gate keeps its budget to the process.
func lockFile(*os.File) error   { return errors.New("no file lock on this system") }
func unlockFile(*os.File) error { return nil }

package processor

import "github.com/pkg/errors"

// FatalErr raise this error to exist processor
type FatalErr interface {
	Fatal() bool
}

func IsFatalErr(err error) bool {
	var fatalErr FatalErr
	return errors.As(err, &fatalErr) && fatalErr.Fatal()
}

type fatalError struct {
	err error
}

func (e fatalError) Error() string {
	return e.err.Error()
}

func (e fatalError) Fatal() bool {
	return true
}

// NewFatalErr wrap original err to create FatalErr
func NewFatalErr(err error) error {
	return &fatalError{err: err}
}

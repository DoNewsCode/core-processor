package processor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsFatalErr(t *testing.T) {
	err := NewFatalErr(fmt.Errorf("test"))
	assert.True(t, IsFatalErr(err))
}

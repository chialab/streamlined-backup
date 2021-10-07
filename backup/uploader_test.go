package backup

import (
	"testing"
	"time"
)

func TestNewHandlerS3(t *testing.T) {
	t.Parallel()

	dest := Destination{
		Type: "s3",
	}
	if handler, err := NewHandler(dest, time.Now()); err != nil {
		t.Errorf("unepected error: %s", err)
	} else if handler == nil {
		t.Errorf("expected handler, got nil")
	}
}

func TestNewHandlerUnknown(t *testing.T) {
	t.Parallel()

	dest := Destination{
		Type: "invalid",
	}
	if handler, err := NewHandler(dest, time.Now()); err == nil {
		t.Error("expected error, got nil")
	} else if handler != nil {
		t.Error("unepected handler")
	}
}

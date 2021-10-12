package backup

import (
	"testing"
)

func TestNewHandlerS3(t *testing.T) {
	t.Parallel()

	dest := Destination{
		Type: "s3",
	}
	if handler, err := NewHandler(dest); err != nil {
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
	if handler, err := NewHandler(dest); err == nil {
		t.Error("expected error, got nil")
	} else if handler != nil {
		t.Error("unepected handler")
	}
}

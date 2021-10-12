package handler

import (
	"testing"

	"github.com/chialab/streamlined-backup/config"
)

func TestNewHandlerS3(t *testing.T) {
	t.Parallel()

	dest := config.Destination{
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

	dest := config.Destination{
		Type: "invalid",
	}
	if handler, err := NewHandler(dest); err == nil {
		t.Error("expected error, got nil")
	} else if handler != nil {
		t.Error("unepected handler")
	}
}

package utils

import (
	"errors"
	"os"
	"os/exec"
	"path"
	"strconv"
	"testing"
)

func TestPidAcquire(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	}

	expected := strconv.Itoa(os.Getpid())
	if data, err := os.ReadFile(pidFile); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if actual := string(data); actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}

func TestPidAcquireStale(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	cmd := exec.Command("true")
	if err := cmd.Start(); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	} else if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	} else if err := cmd.Wait(); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("expected error, got none")
	}

	expected := strconv.Itoa(os.Getpid())
	if data, err := os.ReadFile(pidFile); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if actual := string(data); actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}

func TestPidAcquireRunning(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	cmd := exec.Command("yes")
	if err := cmd.Start(); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	expected := strconv.Itoa(cmd.Process.Pid)
	if err := os.WriteFile(pidFile, []byte(expected), 0644); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}
	defer func() {
		if err := cmd.Process.Signal(os.Kill); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		var exitErr *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exitErr) {
			t.Fatalf("unexpected error: %#v", err)
		}
	}()

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err == nil {
		t.Errorf("expected error, got none")
	} else if err != ErrPidFileExists {
		t.Errorf("expected %#v, got %#v", ErrPidFileExists, err)
	}

	if data, err := os.ReadFile(pidFile); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if actual := string(data); actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}

func TestPidAcquireCorrupt(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")
	if err := os.WriteFile(pidFile, []byte("corrupt"), 0644); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	}

	expected := strconv.Itoa(os.Getpid())
	if data, err := os.ReadFile(pidFile); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if actual := string(data); actual != expected {
		t.Errorf("expected %s, got %s", expected, actual)
	}
}

func TestPidRelease(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be created, got %#v", err)
	}

	if err := pid.Release(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if _, err := os.Stat(pidFile); err == nil || !os.IsNotExist(err) {
		t.Errorf("expected file to be deleted, got %#v", err)
	}
}

func TestPidReleaseCorrupt(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be created, got %#v", err)
	}

	if err := os.WriteFile(pidFile, []byte("corrupt"), 0644); err != nil {
		t.Errorf("unexpected error: %#v", err)
	}

	if err := pid.Release(); err == nil || err != ErrPidFileStale {
		t.Errorf("expected %#v, got %#v", ErrPidFileStale, err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be left untouched, got %#v", err)
	}
}

func TestPidReleasePidChanged(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be created, got %#v", err)
	}

	cmd := exec.Command("yes")
	if err := cmd.Start(); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)), 0644); err != nil {
		t.Fatalf("unexpected error: %#v", err)
	}
	defer func() {
		if err := cmd.Process.Signal(os.Kill); err != nil {
			t.Fatalf("unexpected error: %#v", err)
		}
		var exitErr *exec.ExitError
		if err := cmd.Wait(); err != nil && !errors.As(err, &exitErr) {
			t.Fatalf("unexpected error: %#v", err)
		}
	}()

	if err := pid.Release(); err == nil || err != ErrPidFileStale {
		t.Errorf("expected %#v, got %#v", ErrPidFileStale, err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be left untouched, got %#v", err)
	}
}

func TestPidMustReleaseOk(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be created, got %#v", err)
	}

	pid.MustRelease()
	if _, err := os.Stat(pidFile); err == nil || !os.IsNotExist(err) {
		t.Errorf("expected file to be deleted, got %#v", err)
	}
}

func TestPidMustReleaseError(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	pidFile := path.Join(tmpDir, "file.pid")

	pid := NewPidFile(pidFile)
	if err := pid.Acquire(); err != nil {
		t.Errorf("unexpected error: %#v", err)
	} else if _, err := os.Stat(pidFile); err != nil {
		t.Errorf("expected file to be created, got %#v", err)
	}

	if err := os.WriteFile(pidFile, []byte("corrupt"), 0644); err != nil {
		t.Errorf("unexpected error: %#v", err)
	}

	defer func() {
		if panicked := recover(); panicked == nil || panicked != ErrPidFileStale {
			t.Errorf("expected %#v, got %#v", ErrPidFileStale, panicked)
		} else if _, err := os.Stat(pidFile); err != nil {
			t.Errorf("expected file to be left untouched, got %#v", err)
		}
	}()

	pid.MustRelease()
	t.Fatalf("expected panic")
}

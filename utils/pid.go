package utils

import (
	"errors"
	"log"
	"os"
	"strconv"
	"syscall"
)

var (
	ErrPidFileNotFound = errors.New("pid file not found")
	ErrPidFileStale    = errors.New("pid file exists but points to a not existing process")
	ErrPidFileExists   = errors.New("pid file already exists and points to a valid process")
)

type PidFile struct {
	path string
}

func NewPidFile(path string) *PidFile {
	return &PidFile{path}
}

func (p *PidFile) current() (int, error) {
	if data, err := os.ReadFile(p.path); err != nil {
		return 0, ErrPidFileNotFound
	} else if pid, err := strconv.Atoi(string(data)); err != nil {
		return 0, ErrPidFileStale
	} else if process, err := os.FindProcess(pid); err != nil {
		return 0, ErrPidFileStale
	} else if err := process.Signal(syscall.Signal(0)); err != nil {
		return 0, ErrPidFileStale
	} else {
		return pid, nil
	}
}

func (p *PidFile) Acquire() error {
	if _, err := p.current(); err == nil {
		return ErrPidFileExists
	} else if err == ErrPidFileStale {
		log.Default().Print("pid file exists but points to a not existing process")
	}

	if err := os.WriteFile(p.path, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		return err
	}

	return nil
}

func (p *PidFile) Release() error {
	if pid, err := p.current(); err != nil {
		return err
	} else if pid != os.Getpid() {
		return ErrPidFileStale
	} else if err := os.Remove(p.path); err != nil {
		return err
	}

	return nil
}

func (p *PidFile) MustRelease() {
	if err := p.Release(); err != nil {
		panic(err)
	}
}

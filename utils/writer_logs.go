package utils

import (
	"log"
	"strings"
)

type LogWriter struct {
	currentLine string
	lines       []string
	logger      *log.Logger
}

func NewLogWriter(logger *log.Logger) *LogWriter {
	return &LogWriter{
		logger: logger,
		lines:  make([]string, 0),
	}
}

func (w *LogWriter) Write(p []byte) (n int, err error) {
	data := strings.Split(w.currentLine+string(p), "\n")
	w.currentLine = data[len(data)-1]
	lines := data[:len(data)-1]
	w.lines = append(w.lines, lines...)

	if w.logger == nil {
		return len(p), nil
	}

	for _, line := range lines {
		if err := w.logger.Output(2, line); err != nil {
			return len(p), err
		}
	}

	return len(p), nil
}

func (w *LogWriter) Close() error {
	if w.currentLine == "" {
		return nil
	} else if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}

	return nil
}

func (w LogWriter) Lines() []string {
	return w.lines
}

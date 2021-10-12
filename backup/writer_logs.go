package backup

import (
	"log"
	"strings"
)

type LogWriter struct {
	currentLine string
	Lines       []string
	logger      *log.Logger
}

func (w *LogWriter) Write(p []byte) (n int, err error) {
	data := strings.Split(w.currentLine+string(p), "\n")
	w.currentLine = data[len(data)-1]
	lines := data[:len(data)-1]
	w.Lines = append(w.Lines, lines...)

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

package backup

import "strings"

type logFunction func(string)

type LogWriter struct {
	currentLine string
	log         logFunction
}

func NewLogWriter(log logFunction) *LogWriter {
	return &LogWriter{
		currentLine: "",
		log:         log,
	}
}

func (w *LogWriter) Write(p []byte) (n int, err error) {
	w.Append(string(p))

	return len(p), nil
}

func (w *LogWriter) Append(p string) int {
	lines := strings.Split(w.currentLine+string(p), "\n")
	written := 0
	for _, line := range lines[:len(lines)-1] {
		w.log(line)
		written += len(line)
	}
	w.currentLine = lines[len(lines)-1]

	return written
}

func (w *LogWriter) Close() error {
	if w.currentLine != "" {
		w.log(w.currentLine)
	}

	return nil
}

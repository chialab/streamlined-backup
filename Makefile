all: clean build
.PHONY: all clean lint test

clean:
	@rm -rf build

build:
	@for GOOS in linux darwin; do \
		for GOARCH in amd64 arm64; do \
			go build -o build/streamlined-backup-$${GOOS}-$${GOARCH} -v main.go; \
		done; \
	done

lint:
	@golangci-lint run ./...

test:
	@go test -count=1 -cover -race ./...

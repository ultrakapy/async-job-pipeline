.PHONY: build test run clean

BINARY := bin/server

build:
	go build -o $(BINARY) ./cmd/server

test:
	go test -v -race -timeout 60s ./...

run: build
	./$(BINARY)

clean:
	rm -rf bin/
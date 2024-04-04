all: lint test build

test: lint bench
	go test -timeout=60s -cover -race ./...

bench:
	go test -bench=. -benchtime=3s -benchmem ./...

build:
	# static build for alpine
	CGO_ENABLED=0 go build -ldflags="-w -s" .

lint:
	go fmt ./...
	go vet ./...
	go mod tidy

setup:
	go mod download

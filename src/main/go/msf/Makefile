all: lint test build

test: lint
	go test -timeout=60s -cover -race ./...

bench: test
	go test -bench=. -benchmem -count=3 ./...

build:
	# static build for alpine
	GOOS=linux GOARCH=amd64 GOAMD64=v3 CGO_ENABLED=0 go build -ldflags="-w -s" .

lint:
	go fmt ./...
	go vet ./...
	go mod tidy

setup: 
	go mod download

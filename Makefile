.PHONY: test run

test:
	go test ./...

run:
	go run ./...

fmt:
	gofumpt -l -w .
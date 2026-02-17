.PHONY: mocks
mocks:
	go generate ./...

.PHONY: test
test:
	go test -race -cover ./...

.PHONY: lint
lint:
	golangci-lint run --fix
	
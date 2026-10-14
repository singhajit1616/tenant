.PHONY: up down test lint fmt boot

boot up:
	docker compose up --build -d

down:
	docker compose down -v

fmt:
	gofmt -w .

lint:
	go vet ./...

test:
	docker compose --profile test run --rm --build test


update:
	@echo "Updating dependencies"
	@cat requirements.txt | xargs -l1 go get

build:
	@echo "Create build for Linux"
	mkdir -p build
	GOOS=linux GOARCH=amd64 go build -o build/tabletki main.go

build-windows:
	@echo "Create build for Windows"
	mkdir -p build
	GOOS=windows GOARCH=amd64 go build -o build/tabletki.exe main.go

run-atctree:
	@go run main.go atctree

run-drugs:
	@go run main.go drugs

.PHONY: update build build-windows run-atctree run-drugs

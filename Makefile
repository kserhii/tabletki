
update:
	@echo "Updating dependencies"
	@cat requirements.txt | xargs -l1 go get

build:
	mkdir -p build
	GOOS=linux GOARCH=amd64 go build -o build/tabletki -v main.go

build-windows:
	mkdir -p build
	GOOS=windows GOARCH=amd64 go build -o build/tabletki.exe -v main.go

run:
	@go run main.go


update:
	@echo "Updating dependencies"
	@cat requirements.txt | xargs -l1 go get

run:
	@go run main.go

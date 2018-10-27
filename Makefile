BIN=$(GOPATH)/bin
GB=$(BIN)/gb
build:
	go get github.com/constabulary/gb/...
	$(GB) test
	$(GB) build

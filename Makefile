BIN=$(GOPATH)/bin
GB=$(BIN)/gb
VERSION=`git describe --abbrev=0 --tags`
BUILD_INFO=`git describe --tags`
BUILD_TIME=`date +%FT%T%z`
LDFLAGS=-ldflags "-v -w -s -X main.Version=${VERSION} -X main.BuildID=${BUILD_INFO} -X main.BuildDate=${BUILD_TIME}"

deps:
	go get github.com/constabulary/gb/..
build:
	$(GB) test
	$(GB) build ${LDFLAGS}

.PHONY: deps

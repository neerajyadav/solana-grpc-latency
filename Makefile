BINARY_NAME := grpc-latency-test
BUILD_DIR := build
# Go command variables
GOCMD := go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGET := $(GOCMD) get
GOMOD := $(GOCMD) mod

LDFLAGS := -ldflags "-X main.Version=$(VERSION)"

.PHONY: clean build build-linux

build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) main.go

build-linux:
	mkdir -p $(BUILD_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux main.go

# Clean the project
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
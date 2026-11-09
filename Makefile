GO ?= go
GOEXPERIMENT ?= jsonv2,greenteagc
DIST ?= dist
PACKAGE := ./cmd/adaptihound-se
SOURCES := $(wildcard cmd/adaptihound-se/*.go internal/merge/*.go)

.PHONY: all check clean
all: $(DIST)/AdaptiHound-SE.so $(DIST)/config.yaml $(DIST)/ax_config.axs

$(DIST):
	@mkdir -p "$@"

$(DIST)/AdaptiHound-SE.so: $(SOURCES) go.mod go.sum | $(DIST)
	GOEXPERIMENT=$(GOEXPERIMENT) $(GO) build -buildmode=plugin -buildvcs=false -ldflags="-s -w" -o "$@" $(PACKAGE)

$(DIST)/config.yaml: config.yaml | $(DIST)
	@cp "$<" "$@"

$(DIST)/ax_config.axs: ax_config.axs | $(DIST)
	@cp "$<" "$@"

check:
	GOEXPERIMENT=$(GOEXPERIMENT) $(GO) vet ./...

clean:
	rm -rf -- "$(DIST)"

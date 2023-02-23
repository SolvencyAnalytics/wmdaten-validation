MAKEFILE := $(firstword $(MAKEFILE_LIST))
export PYTHON_VERSION := 3.10
export VALIDATION_DOCKER_IMAGE := wmdaten-validation

.PHONY: help
help: ## Shows this help
	@echo "Targets:"
	@fgrep -h "##" $(MAKEFILE) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/\(.*\):.*##[ \t]*/    \1 ## /' | column -t -s '##'
	@echo

.PHONY: build
build:  ## Builds the image
	docker build .\
		--build-arg	PYTHON_VERSION=$(PYTHON_VERSION) \
		-t $(VALIDATION_DOCKER_IMAGE) \
		-f Dockerfile

.PHONY: shell
shell: build ## Creates shell for e2e tester
	docker run --rm -it -v `pwd`:/app $(VALIDATION_DOCKER_IMAGE) /bin/bash

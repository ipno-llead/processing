SHELL = /bin/bash
GSUTIL = gsutil

OS := $(shell uname -s)
BUILD_DIR := build

.DEFAULT_GOAL := all

.SUFFIXES:
.SECONDARY:

define check_var
@[ "$($(1))" ] || ( echo "$(1) is not set"; exit 1 )
endef

$(BUILD_DIR): ; @-mkdir $@ 2>/dev/null

schema.md: $(MD5_DIR)/data/datavalid.yml.md5
	python -m datavalid --dir $(DATA_DIR) --doc $@

include deba.mk
# include wrgl.mk

.PHONY: all
all: deba $(DEBA_DATA_DIR)/fuse/person.csv

.PHONY: clean
clean: cleandeba
	rm -rf $(BUILD_DIR)

.PHONY: ocr_results
ocr_results:
	$(GSUTIL) -m rsync -i -J -r gs://k8s-ocr-jobqueue-results/ocr/ ${DEBA_DATA_DIR}/ocr_results

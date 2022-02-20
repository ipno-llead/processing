SHELL = /bin/bash

OS := $(shell uname -s)
BUILD_DIR := build

.DEFAULT_GOAL := all

.SUFFIXES:
.SECONDARY:
.PHONY: all clean

define check_var
@[ "$($(1))" ] || ( echo "$(1) is not set"; exit 1 )
endef

$(BUILD_DIR): ; @-mkdir $@ 2>/dev/null

schema.md: $(MD5_DIR)/data/datavalid.yml.md5
	python -m datavalid --dir $(DATA_DIR) --doc $@

include bolo.mk
include wrgl.mk

all: bolo $(BOLO_DATA_DIR)/fuse/person.csv
clean: cleanbolo
	rm -rf $(BUILD_DIR)

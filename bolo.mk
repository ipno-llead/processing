SHELL = /bin/bash

OS := $(shell uname -s)
BOLO_MD5 := $(if $(findstring Darwin,$(OS)),md5,md5sum)
PYTHON := python
BOLO_DIR := .bolo
BOLO_FILE := bolo.yaml

BOLO_DEPS_DIR := $(BOLO_DIR)/deps
BOLO_DATA_DIR := $(shell $(PYTHON) -m bolo dataDir)
BOLO_MD5_DIR := $(BOLO_DIR)/md5
BOLO_STAGES := $(shell $(PYTHON) -m bolo stages)
BOLO_DEP_FILES := $(patsubst %,$(BOLO_DEPS_DIR)/%.d,$(BOLO_STAGES))
BOLO_PYTHON_PATH := $(shell $(PYTHON) -m bolo pythonPath)

.PHONY: bolo cleanbolo

bolo: $(shell $(PYTHON) -m bolo targets)

cleanbolo:
	rm -rf $(BOLO_DIR)

define bolo_execute
@start_time=$$SECONDS && \
echo "running $(1)" | sed $$'s,.*,\e[1;37m&\e[m,' && \
set -o pipefail && \
(PYTHONPATH=$(BOLO_PYTHON_PATH) $(PYTHON) $(1) 2>&1>&3 | sed $$'s,.*,    \e[31m&\e[m,' >&2 )3>&1 | sed $$'s,.*,    \e[1;30m&\e[m,' && \
echo "    script completed in $$((SECONDS - start_time)) seconds" | sed $$'s,.*,\e[1;34m&\e[m,'
endef

# calculate md5
$(BOLO_MD5_DIR)/%.md5: % | $(BOLO_MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(BOLO_MD5) $<)),$(BOLO_MD5) $< > $@)

$(BOLO_DEPS_DIR)/%.d: %/*.py $(BOLO_FILE) | $(BOLO_DEPS_DIR)
	$(PYTHON) -m bolo deps --stage $*

$(BOLO_DIR)/main.d: $(BOLO_FILE) | $(BOLO_DIR)
	$(PYTHON) -m bolo deps

$(BOLO_DIR): ; @-mkdir $@ 2>/dev/null
$(BOLO_DATA_DIR): ; @-mkdir -p $@ 2>/dev/null
$(BOLO_DEPS_DIR) $(BOLO_MD5_DIR): | $(BOLO_DIR) ; @-mkdir $@ 2>/dev/null

include $(BOLO_DIR)/main.d
include $(BOLO_DEP_FILES)
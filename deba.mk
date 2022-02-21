SHELL = /bin/bash

OS := $(shell uname -s)
DEBA_MD5 := $(if $(findstring Darwin,$(OS)),md5,md5sum)
PYTHON := python
DEBA_DIR := .deba
DEBA_FILE := deba.yaml

DEBA_DEPS_DIR := $(DEBA_DIR)/deps
DEBA_DATA_DIR := $(shell $(PYTHON) -m deba dataDir)
DEBA_MD5_DIR := $(DEBA_DIR)/md5
DEBA_STAGES := $(shell $(PYTHON) -m deba stages)
DEBA_DEP_FILES := $(patsubst %,$(DEBA_DEPS_DIR)/%.d,$(DEBA_STAGES))
DEBA_PYTHON_PATH := $(shell $(PYTHON) -m deba pythonPath)

.PHONY: deba cleandeba

deba: $(shell $(PYTHON) -m deba targets)

cleandeba:
	rm -rf $(DEBA_DIR)

define deba_execute
@start_time=$$SECONDS && \
echo "running $(1)" | sed $$'s,.*,\e[1;37m&\e[m,' && \
set -o pipefail && \
(PYTHONPATH=$(DEBA_PYTHON_PATH) $(PYTHON) $(1) 2>&1>&3 | sed $$'s,.*,    \e[31m&\e[m,' >&2 )3>&1 | sed $$'s,.*,    \e[1;30m&\e[m,' && \
echo "    script completed in $$((SECONDS - start_time)) seconds" | sed $$'s,.*,\e[1;34m&\e[m,'
endef

# calculate md5
$(DEBA_MD5_DIR)/%.md5: % | $(DEBA_MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(DEBA_MD5) $<)),$(DEBA_MD5) $< > $@)

$(DEBA_DEPS_DIR)/%.d: %/*.py $(DEBA_FILE) | $(DEBA_DEPS_DIR)
	$(PYTHON) -m deba deps --stage $*

$(DEBA_DIR)/main.d: $(DEBA_FILE) | $(DEBA_DIR)
	$(PYTHON) -m deba deps

$(DEBA_DIR): ; @-mkdir $@ 2>/dev/null
$(DEBA_DATA_DIR): ; @-mkdir -p $@ 2>/dev/null
$(DEBA_DEPS_DIR) $(DEBA_MD5_DIR): | $(DEBA_DIR) ; @-mkdir $@ 2>/dev/null

include $(DEBA_DIR)/main.d
include $(DEBA_DEP_FILES)
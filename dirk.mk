SHELL = /bin/bash

OS := $(shell uname -s)
PYTHON := python
DIRK_DIR := .dirk
DIRK_FILE := dirk.yaml

DIRK_DEPS_DIR := $(DIRK_DIR)/deps
DIRK_DATA_DIR := $(shell $(PYTHON) -m dirk dataDir)
DIRK_MD5_DIR := $(DIRK_DIR)/md5
DIRK_STAGES := $(shell $(PYTHON) -m dirk stages)
DIRK_DEP_FILES := $(patsubst %,$(DIRK_DEPS_DIR)/%.d,$(DIRK_STAGES))
DIRK_PYTHON_PATH := $(shell $(PYTHON) -m dirk pythonPath)

.PHONY: dirk cleandirk

dirk: $(shell $(PYTHON) -m dirk targets)

cleandirk:
	rm -rf $(DIRK_DIR)

define dirk_execute
@start_time=$$SECONDS && \
echo "running $(1)" | sed $$'s,.*,\e[1;37m&\e[m,' && \
(set -o pipefail; PYTHONPATH=$(DIRK_PYTHON_PATH) $(PYTHON) $(1) 2>&1>&3 | sed $$'s,.*,    \e[31m&\e[m,' >&2 )3>&1 | sed $$'s,.*,    \e[1;30m&\e[m,' && \
echo "    script completed in $$((SECONDS - start_time)) seconds" | sed $$'s,.*,\e[1;34m&\e[m,'
endef

# calculate md5
$(DIRK_MD5_DIR)/%.md5: % | $(DIRK_MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	@$(PYTHON) -m dirk md5 $< $@

$(DIRK_DEPS_DIR)/%.d: %/*.py $(DIRK_FILE) | $(DIRK_DEPS_DIR)
	$(PYTHON) -m dirk deps --stage $*

$(DIRK_DIR)/main.d: $(DIRK_FILE) | $(DIRK_DIR)
	$(PYTHON) -m dirk deps

$(DIRK_DIR): ; @-mkdir $@ 2>/dev/null
$(DIRK_DATA_DIR): ; @-mkdir -p $@ 2>/dev/null
$(DIRK_DEPS_DIR) $(DIRK_MD5_DIR): | $(DIRK_DIR) ; @-mkdir $@ 2>/dev/null

include $(DIRK_DIR)/main.d
include $(DIRK_DEP_FILES)
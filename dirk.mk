SHELL = /bin/bash

OS := $(shell uname -s)
PYTHON := python
DIRK_DIR := .dirk
DIRK_FILE := dirk.yaml

DEPS_DIR := $(DIRK_DIR)/deps
DATA_DIR := $(shell $(PYTHON) -m dirk dataDir)
MD5_DIR := $(DIRK_DIR)/md5
STAGES := $(shell $(PYTHON) -m dirk stages)
DEP_FILES := $(patsubst %,$(DEPS_DIR)/%.d,$(STAGES))
PYTHON_PATH := $(shell $(PYTHON) -m dirk pythonPath)

.PHONY: dirk cleandirk

dirk: $(shell $(PYTHON) -m dirk targets)

cleandirk:
	rm -rf $(DIRK_DIR)

define execute
start_time=$$SECONDS && \
PYTHONPATH=$(PYTHON_PATH) && \
echo "running $(1)" | sed $$'s,.*,\e[1;37m&\e[m,' && \
(set -o pipefail; $(PYTHON) $(1) 2>&1>&3 | sed $$'s,.*,  \e[31m&\e[m,' >&2 )3>&1 | sed $$'s,.*,  \e[1;30m&\e[m,' && \
echo "script completed in $$((SECONDS - start_time)) seconds" | sed $$'s,.*,\e[1;34m&\e[m,'
endef

# calculate md5
$(MD5_DIR)/%.md5: % | $(MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(PYTHON) -m dirk md5 $< $@

$(DEPS_DIR)/%.d: %/*.py $(DIRK_FILE) | $(DEPS_DIR)
	$(PYTHON) -m dirk deps --stage $*

$(DIRK_DIR)/main.d: $(DIRK_FILE) | $(DIRK_DIR)
	$(PYTHON) -m dirk deps

$(DIRK_DIR): ; @-mkdir $@ 2>/dev/null
$(DATA_DIR): ; @-mkdir -p $@ 2>/dev/null
$(DEPS_DIR) $(MD5_DIR): | $(DIRK_DIR) ; @-mkdir $@ 2>/dev/null

include $(DIRK_DIR)/main.d
include $(DEP_FILES)
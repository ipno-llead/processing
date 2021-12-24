SHELL = /bin/bash

OS := $(shell uname -s)
MD5 := $(if $(findstring Darwin,$(OS)),md5,md5sum)
BUILD_DIR := build
MD5_DIR := $(BUILD_DIR)/md5
DATA_DIR := data
DATA_CLEAN_DIR := $(DATA_DIR)/clean
DATA_MATCH_DIR := $(DATA_DIR)/match
DATA_FUSE_DIR := $(DATA_DIR)/fuse
SCRIPT_DIRS := clean match fuse
DATA_DEP_FILES := $(patsubst %,%/data.d,$(SCRIPT_DIRS))
EXCLUDED_SCRIPTS := fuse/all.py match/cross_agency.py match/harahan_pd.py

.DEFAULT_GOAL := all

export PYTHONPATH := $(shell pwd):$(PYTHONPATH)

.SUFFIXES:
.SECONDARY:
.PHONY: all clean

all: download_links $(BUILD_DIR)/.fuse-all $(DATA_MATCH_DIR)/person.csv
clean:
	rm -f $(DATA_DEP_FILES)
	rm -rf $(BUILD_DIR)

define check_var
@[ "$($(1))" ] || ( echo "$(1) is not set"; exit 1 )
endef

$(BUILD_DIR)/.fuse-all: $(MD5_DIR)/fuse/all.py.md5
	scripts/run.sh fuse/all.py
	@touch $@

# calculate md5
$(MD5_DIR)/%.md5: % | $(MD5_DIR)
	@-mkdir -p $(dir $@) 2>/dev/null
	$(if $(filter-out $(shell cat $@ 2>/dev/null),$(shell $(MD5) $<)),$(MD5) $< > $@)

%/data.d: %/*.py
	scripts/write_deps.py $* $(if $(findstring fuse,$*),--all --dependency build/md5/data/datavalid.yml.md5 ,)$(patsubst %,-e %,$(EXCLUDED_SCRIPTS))

$(BUILD_DIR) $(DATA_DIR): ; @-mkdir $@ 2>/dev/null
$(MD5_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null
$(DATA_CLEAN_DIR) $(DATA_MATCH_DIR) $(DATA_FUSE_DIR): | $(DATA_DIR) ; @-mkdir $@ 2>/dev/null

schema.md: $(MD5_DIR)/data/datavalid.yml.md5
	python -m datavalid --dir $(DATA_DIR) --doc $@

include raw_datasets.mk
include $(DATA_DEP_FILES)
include wrgl.mk

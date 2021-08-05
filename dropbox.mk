DROPBOX_DIR := $(DATA_DIR)/dropbox
DROPBOX_LINKS_DIR := $(BUILD_DIR)/dropbox
CURL := curl

.PHONY: download_deps

download_deps: | $(DROPBOX_LINKS_DIR)
	scripts/write_download_deps.py dropbox_links.json $(DROPBOX_LINKS_DIR)

$(DROPBOX_DIR)/%.csv: $(DROPBOX_LINKS_DIR)/%.csv.link | $(DROPBOX_DIR) download_deps
	-mkdir $(dir $@) 2>/dev/null
	@cd $(dir $@) && { $(CURL) -f -L $(shell cat $<) -o $(notdir $@) || { echo "link isn't working: $(shell cat $<)"; exit 22; } }

$(DROPBOX_DIR): | $(DATA_DIR) ; @-mkdir $@ 2>/dev/null
$(DROPBOX_LINKS_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null

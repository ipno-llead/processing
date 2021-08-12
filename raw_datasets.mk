RAW_DIR := $(DATA_DIR)/raw
RAW_LINKS_DIR := $(BUILD_DIR)/raw
CURL := curl

.PHONY: download_links

download_links: | $(RAW_LINKS_DIR)
	scripts/write_download_links.py raw_datasets.json $(RAW_LINKS_DIR)

$(RAW_DIR)/%.csv: $(RAW_LINKS_DIR)/%.csv.link | $(RAW_DIR) download_links
	-mkdir $(dir $@) 2>/dev/null
	@cd $(dir $@) && { $(CURL) -f -L $(shell cat $<) -o $(notdir $@) || { echo "link isn't working: $(shell cat $<)"; exit 22; } }

$(RAW_DIR): | $(DATA_DIR) ; @-mkdir $@ 2>/dev/null
$(RAW_LINKS_DIR): | $(BUILD_DIR) ; @-mkdir $@ 2>/dev/null

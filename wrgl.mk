WRGL := wrgl

pull_person:
	@if [[ ! -f $(BUILD_DIR)/person.csv || "$(shell wrgl pull person)" != *"Already up to date."* ]]; then \
		$(WRGL) export person > $(BUILD_DIR)/person.csv && \
		echo 'exported person branch to file $(BUILD_DIR)/person.csv'; \
	else \
		echo 'file $(BUILD_DIR)/person.csv is up to date.'; \
	fi

$(DEBA_DATA_DIR)/fuse/person.csv: $(DEBA_MD5_DIR)/fuse/cross_agency.py.md5 $(DEBA_DATA_DIR)/fuse/personnel.csv $(DEBA_DATA_DIR)/fuse/event.csv | pull_person
	$(call deba_execute,fuse/cross_agency.py $(BUILD_DIR)/person.csv)

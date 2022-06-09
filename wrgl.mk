WRGL := wrgl

pull_person: | $(BUILD_DIR)
	@if [[ ! -f $(BUILD_DIR)/person.csv || "$(shell $(WRGL) pull person origin +refs/heads/person:refs/remotes/origin/person $(WRGL_FLAGS))" != *"Already up to date."* ]]; then \
		$(WRGL) export $(WRGL_FLAGS) person > $(BUILD_DIR)/person.csv && \
		echo 'exported person branch to file $(BUILD_DIR)/person.csv'; \
	else \
		echo 'file $(BUILD_DIR)/person.csv is up to date.'; \
	fi

$(DEBA_DATA_DIR)/fuse/person.csv: $(DEBA_MD5_DIR)/fuse/cross_agency.py.md5 $(DEBA_DATA_DIR)/fuse/personnel.csv $(DEBA_DATA_DIR)/fuse/event.csv | pull_person
	$(call deba_execute,fuse/cross_agency.py $(BUILD_DIR)/person.csv)

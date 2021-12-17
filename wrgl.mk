WRGL := wrgl

pull_person:
	if [[ ! -f $(BUILD_DIR)/person.csv || "$(shell wrgl pull person)" != *"Already up to date."* ]]; then \
		$(WRGL) export person > $(BUILD_DIR)/person.csv && \
		echo 'exported person branch to file $(BUILD_DIR)/person.csv'; \
	else \
		echo 'file $(BUILD_DIR)/person.csv is up to date.'; \
	fi

$(DATA_MATCH_DIR)/person.csv: $(BUILD_DIR)/person.csv $(MD5_DIR)/match/cross_agency.py.md5 $(BUILD_DIR)/.fuse-all | pull_person
	scripts/run.sh match/cross_agency.py $<

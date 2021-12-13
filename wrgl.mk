WRGL := wrgl
REPOS := personnel person allegation use-of-force stop-and-search event
REPO_DIRS := $(foreach repo,$(REPOS),.wrglfleet/$(repo))
COMBINED_FILE_NAMES := personnel.csv event.csv allegation.csv use_of_force.csv stop_and_search.csv
COMBINED_FILE_PATHS := $(foreach name,$(COMBINED_FILE_NAMES),$(DATA_FUSE_DIR)/$(name))

.PHONY: pull_all commit_all push_all

define commit
scripts/wrgl_commit.py $(1) main $(2)

endef

define pull
$(WRGL) pull main --wrgl-dir .wrglfleet/$(1)

endef

define print_diff_cmd
@echo '  wrgl diff --wrgl-dir .wrglfleet/$(1) main'

endef

define push
$(WRGL) push origin refs/heads/main:refs/heads/main --wrgl-dir .wrglfleet/$(1)

endef

pull_all: $(REPO_DIRS)
	$(foreach repo,$(REPOS),$(call pull,$(repo)))

pull_person: | .wrglfleet/person
	if [[ "$(shell ($(call pull,person)))" != *"Already up to date."* ]]; then \
		wrgl export --wrgl-dir .wrglfleet/person main > $(BUILD_DIR)/person.csv; \
	else \
		echo 'file $(BUILD_DIR)/person.csv is up to date.'; \
	fi

$(DATA_MATCH_DIR)/person.csv: $(BUILD_DIR)/person.csv $(MD5_DIR)/match/cross_agency.py.md5 $(BUILD_DIR)/.fuse-all | pull_person
	scripts/run.sh match/cross_agency.py $<

commit_all: $(DATA_MATCH_DIR)/person.csv $(BUILD_DIR)/.fuse-all
	$(foreach repo,$(REPOS),$(call commit,$(repo),$(MESSAGE)))
	@-echo 'use the following commands to inspect changes:'
	$(foreach repo,$(REPOS),$(call print_diff_cmd,$(repo)))

push_all:
	$(foreach repo,$(REPOS),$(call push,$(repo)))

pull_%: | .wrglfleet/%
	$(call pull,$*)

commit_%: | .wrglfleet/%
	$(call commit,$*,$(MESSAGE))

push_%: commit_%
	$(call push,$*)

.wrglfleet/%: | .wrglfleet
	$(WRGL) init --wrgl-dir $@
	$(WRGL) remote add origin https://hub.wrgl.co/api/users/ipno/repos/$*/ --wrgl-dir $@
	$(WRGL) pull main origin refs/heads/main:refs/remotes/origin/main --set-upstream --wrgl-dir $@

.wrglfleet: ; @-mkdir $@ 2>/dev/null


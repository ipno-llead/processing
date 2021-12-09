WRGL := wrgl
REPOS := personnel person allegation use-of-force stop-and-search event
REPO_DIRS := $(foreach repo,$(REPOS),.wrglfleet/$(repo))
COMBINED_FILE_NAMES := personnel.csv event.csv allegation.csv use_of_force.csv stop_and_search.csv
COMBINED_FILE_PATHS := $(foreach name,$(COMBINED_FILE_NAMES),$(DATA_FUSE_DIR)/$(name))

.PHONY: pull_all commit_all push_all

define commit
$(WRGL) branch -d tmp --wrgl-dir $(1)
scripts/commit.py $(1) tmp

endef

define pull
$(WRGL) pull main --wrgl-dir .wrglfleet/$(1)

endef

define print_diff_cmd
echo '  wrgl diff --wrgl-dir .wrglfleet/$(1) tmp main'

endef

define push
$(WRGL) push origin refs/heads/tmp:refs/heads/main --wrgl-dir .wrglfleet/$(1)

endef

pull_all: $(REPO_DIRS)
	$(foreach repo,$(REPOS),$(call pull,$(repo)))

$(BUILD_DIR)/person.csv: pull_person
	wrgl export --wrgl-dir .wrglfleet/person main > $@

$(BUILD_DIR)/.fuse-all: all
	scripts/run.sh fuse/all.py
	@-python -m datavalid --dir data
	@touch $@

$(DATA_MATCH_DIR)/person.csv: $(BUILD_DIR)/person.csv match/cross_agency.py $(BUILD_DIR)/.fuse-all pull_person
	scripts/run.sh match/cross_agency.py $<

commit_all: $(DATA_MATCH_DIR)/person.csv pull_all
	$(foreach repo,$(REPOS),$(call commit,$(repo)))
	@-echo 'use the following commands to inspect changes:'
	$(foreach repo,$(REPOS),$(call print_diff_cmd,$(repo)))

push_all:
	$(foreach repo,$(REPOS),$(call push,$(repo)))

pull_%: .wrglfleet/%
	$(call pull,$*)

commit_%: .wrglfleet/%
	$(call commit,$*)

push_%: commit_%
	$(call push,$*)

.wrglfleet/%: | .wrglfleet
	$(WRGL) init --wrgl-dir $@
	$(WRGL) remote add origin https://hub.wrgl.co/api/users/ipno/repos/$*/ --wrgl-dir $@
	$(WRGL) pull main origin refs/heads/main:refs/remotes/origin/main --set-upstream --wrgl-dir $@

.wrglfleet: ; @-mkdir $@ 2>/dev/null


DVC := dvc

RAW_MINUTES_DIR := $(shell cat $(DEBA_DATA_DIR)/raw_minutes.dvc | grep "md5" | sed -E "s/- md5: (.{2})(.+)/.dvc\/cache\/\1\/\2/")

$(RAW_MINUTES_DIR):
	if [ ! -f "$@" ]; then
		$(DVC) pull
	fi

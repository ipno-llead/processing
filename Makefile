current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/clean/pprr_new_orleans_harbor_pd_2020.csv data/clean/pprr_new_orleans_pd.csv data/clean/pprr_new_orleans_csd_2009.csv data/clean/pprr_new_orleans_csd_2014.csv

data/clean/pprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd_pprr.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv
	python clean/new_orleans_harbor_pd_pprr.py

data/clean/pprr_new_orleans_pd.csv: clean/new_orleans_pd_pprr.py data/new_orleans_pd/*.csv
	python clean/new_orleans_pd_pprr.py

data/clean/pprr_new_orleans_csd_2009.csv data/clean/pprr_new_orleans_csd_2014.csv: clean/new_orleans_csd_pprr.py data/new_orleans_csd/*.csv
	python clean/new_orleans_csd_pprr.py
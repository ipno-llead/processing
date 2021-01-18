current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/clean/pprr_new_orleans_harbor_pd_2020.csv data/clean/pprr_new_orleans_pd.csv

data/clean/pprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv
	python clean/new_orleans_harbor_pd.py

data/clean/pprr_new_orleans_pd.csv: clean/new_orleans_pd.py data/new_orleans_pd/*.csv
	python clean/new_orleans_pd.py
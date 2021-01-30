current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv

data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv: fuse/new_orleans_pd.py data/clean/pprr_new_orleans_pd.csv
	python fuse/new_orleans_pd.py

data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv: fuse/new_orleans_harbor_pd.py data/clean/pprr_new_orleans_harbor_pd_2020.csv
	python fuse/new_orleans_harbor_pd.py

data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv: fuse/baton_rouge_pd.py data/clean/cprr_baton_rouge_pd_2018.csv
	python fuse/baton_rouge_pd.py

data/clean/pprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd_pprr.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv
	python clean/new_orleans_harbor_pd_pprr.py

data/clean/pprr_new_orleans_pd.csv: clean/new_orleans_pd_pprr.py data/new_orleans_pd/*.csv
	python clean/new_orleans_pd_pprr.py

data/clean/pprr_new_orleans_csd_2009.csv data/clean/pprr_new_orleans_csd_2014.csv: clean/new_orleans_csd_pprr.py data/new_orleans_csd/*.csv
	python clean/new_orleans_csd_pprr.py

data/clean/pprr_baton_rouge_csd_2017.csv data/clean/pprr_baton_rouge_csd_2019.csv: clean/baton_rouge_csd_pprr.py data/baton_rouge_csd/*.csv
	python clean/baton_rouge_csd_pprr.py

data/clean/cprr_baton_rouge_pd_2018.csv: clean/baton_rouge_pd_cprr.py data/baton_rouge_pd/baton_rouge_pd_cprr_2018.csv
	python clean/baton_rouge_pd_cprr.py
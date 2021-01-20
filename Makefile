current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/fuse/personel.csv data/fuse/personel_history.csv data/fuse/complaint.csv data/clean/pprr_new_orleans_csd_2009.csv data/clean/pprr_new_orleans_csd_2014.csv data/clean/pprr_baton_rouge_csd_2017.csv data/clean/pprr_baton_rouge_csd_2019.csv

data/fuse/personel.csv data/fuse/personel_history.csv data/fuse/complaint.csv: fuse/all.py data/clean/pprr_new_orleans_harbor_pd_2020.csv data/clean/pprr_new_orleans_pd.csv data/clean/cprr_baton_rouge_pd_2018.csv
	python fuse/all.py

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
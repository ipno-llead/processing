current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv data/fuse/per_baton_rouge_so.csv data/fuse/perhist_baton_rouge_so.csv data/fuse/com_baton_rouge_so.csv data/fuse/per_new_orleans_csd.csv data/fuse/perhist_new_orleans_csd.csv data/fuse/per_baton_rouge_csd.csv data/fuse/perhist_baton_rouge_csd.csv

data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv: fuse/new_orleans_pd.py data/clean/pprr_new_orleans_pd.csv
	python fuse/new_orleans_pd.py

data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv: fuse/new_orleans_harbor_pd.py data/clean/pprr_new_orleans_harbor_pd_2020.csv data/match/cprr_new_orleans_harbor_pd_2020.csv
	python fuse/new_orleans_harbor_pd.py

data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv: fuse/baton_rouge_pd.py data/clean/cprr_baton_rouge_pd_2018.csv
	python fuse/baton_rouge_pd.py

data/fuse/per_baton_rouge_so.csv data/fuse/perhist_baton_rouge_so.csv data/fuse/com_baton_rouge_so.csv: fuse/baton_rouge_so.py data/clean/cprr_baton_rouge_so_2018.csv
	python fuse/baton_rouge_so.py

data/fuse/per_new_orleans_csd.csv data/fuse/perhist_new_orleans_csd.csv: fuse/new_orleans_csd.py data/match/pprr_new_orleans_csd_2009.csv data/match/pprr_new_orleans_csd_2014.csv
	python fuse/new_orleans_csd.py

data/fuse/per_baton_rouge_csd.csv data/fuse/perhist_baton_rouge_csd.csv: fuse/baton_rouge_csd.py data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv
	python fuse/baton_rouge_csd.py



data/match/cprr_new_orleans_harbor_pd_2020.csv: match/new_orleans_harbor_pd.py data/clean/cprr_new_orleans_harbor_pd_2020.csv data/clean/pprr_new_orleans_harbor_pd_2020.csv
	python match/new_orleans_harbor_pd.py

data/match/pprr_new_orleans_csd_2009.csv data/match/pprr_new_orleans_csd_2014.csv: match/new_orleans_csd.py data/clean/pprr_new_orleans_csd_2009.csv data/clean/pprr_new_orleans_csd_2014.csv
	python match/new_orleans_csd.py

data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv: match/baton_rouge_csd.py data/clean/pprr_baton_rouge_csd_2017.csv data/clean/pprr_baton_rouge_csd_2019.csv
	python match/baton_rouge_csd.py



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

data/clean/cprr_baton_rouge_so_2018.csv: clean/baton_rouge_so_cprr.py data/baton_rouge_so/baton_rouge_so_cprr_2018.csv
	python clean/baton_rouge_so_cprr.py

data/clean/cprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd_cprr.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_cprr_2014-2020.csv
	python clean/new_orleans_harbor_pd_cprr.py
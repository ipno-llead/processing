current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv
all: data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv
all: data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv
all: data/fuse/per_baton_rouge_so.csv data/fuse/perhist_baton_rouge_so.csv data/fuse/com_baton_rouge_so.csv
all: data/fuse/per_new_orleans_csd.csv data/fuse/perhist_new_orleans_csd.csv
all: data/fuse/per_brusly_pd.csv data/fuse/perhist_brusly_pd.csv data/fuse/com_brusly_pd.csv
all: data/fuse/per_port_allen_pd.csv data/fuse/perhist_port_allen_pd.csv data/fuse/com_port_allen_pd.csv
all: data/fuse/per_madisonville_pd.csv data/fuse/perhist_madisonville_pd.csv data/fuse/com_madisonville_pd.csv

data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv: fuse/new_orleans_pd.py data/clean/pprr_new_orleans_pd.csv
	python fuse/new_orleans_pd.py

data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv: fuse/new_orleans_harbor_pd.py data/clean/pprr_new_orleans_harbor_pd_2020.csv data/match/cprr_new_orleans_harbor_pd_2020.csv
	python fuse/new_orleans_harbor_pd.py

data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv: fuse/baton_rouge_pd.py data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv data/match/cprr_baton_rouge_pd_2018.csv
	python fuse/baton_rouge_pd.py

data/fuse/per_baton_rouge_so.csv data/fuse/perhist_baton_rouge_so.csv data/fuse/com_baton_rouge_so.csv: fuse/baton_rouge_so.py data/clean/cprr_baton_rouge_so_2018.csv
	python fuse/baton_rouge_so.py

data/fuse/per_baton_rouge_csd.csv data/fuse/perhist_baton_rouge_csd.csv: fuse/baton_rouge_csd.py data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv
	python fuse/baton_rouge_csd.py

data/fuse/per_brusly_pd.csv data/fuse/perhist_brusly_pd.csv data/fuse/com_brusly_pd.csv: fuse/brusly_pd.py data/clean/pprr_brusly_pd_2020.csv data/match/cprr_brusly_pd_2020.csv
	python fuse/brusly_pd.py

data/fuse/per_port_allen_pd.csv data/fuse/perhist_port_allen_pd.csv data/fuse/com_port_allen_pd.csv: fuse/port_allen_pd.py data/clean/cprr_port_allen_pd_2019.csv
	python fuse/port_allen_pd.py

data/fuse/per_madisonville_pd.csv data/fuse/perhist_madisonville_pd.csv data/fuse/com_madisonville_pd.csv: fuse/madisonville_pd.py data/match/cprr_madisonville_pd_2010_2020.csv data/clean/pprr_madisonville_csd_2019.csv
	python fuse/madisonville_pd.py



data/match/cprr_new_orleans_harbor_pd_2020.csv: match/new_orleans_harbor_pd.py data/clean/cprr_new_orleans_harbor_pd_2020.csv data/clean/pprr_new_orleans_harbor_pd_2020.csv
	python match/new_orleans_harbor_pd.py

data/match/pprr_new_orleans_csd_2009.csv data/match/pprr_new_orleans_csd_2014.csv: match/new_orleans_csd.py data/clean/pprr_new_orleans_csd_2009.csv data/clean/pprr_new_orleans_csd_2014.csv
	python match/new_orleans_csd.py

data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv data/match/cprr_baton_rouge_pd_2018.csv: match/baton_rouge.py data/clean/pprr_baton_rouge_csd_2017.csv data/clean/pprr_baton_rouge_csd_2019.csv data/clean/cprr_baton_rouge_pd_2018.csv
	python match/baton_rouge.py

data/match/cprr_brusly_pd_2020.csv: match/brusly_pd.py data/clean/pprr_brusly_pd_2020.csv data/clean/cprr_brusly_pd_2020.csv
	python match/brusly_pd.py

data/match/cprr_madisonville_pd_2010_2020.csv: match/madisonville_pd.py data/clean/cprr_madisonville_pd_2010_2020.csv data/clean/pprr_madisonville_csd_2019.csv
	python match/madisonville_pd.py



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

data/clean/lprr_louisiana_state_csc_1991_2020.csv: clean/louisiana_state_csc_lprr.py data/louisiana_state_csc/louisianastate_csc_lprr_1991-2020.csv
	python clean/louisiana_state_csc_lprr.py

data/clean/lprr_baton_rouge_fpcsb_1992_2012.csv: clean/baton_rouge_fpcsb_lprr.py data/baton_rouge_fpcsb/baton_rouge_fpcsb_logs_1992-2012.csv
	python clean/baton_rouge_fpcsb_lprr.py

data/clean/pprr_brusly_pd_2020.csv data/clean/cprr_brusly_pd_2020.csv: clean/brusly_pd.py data/brusly_pd/brusly_pd_pprr_2020.csv data/brusly_pd/brusly_pd_cprr_2020.csv
	python clean/brusly_pd.py

data/clean/cprr_port_allen_pd_2019.csv: clean/port_allen_pd_cprr.py data/port_allen_pd/port_allen_cprr_2019.csv
	python clean/port_allen_pd_cprr.py

data/clean/cprr_madisonville_pd_2010_2020.csv: clean/madisonville_pd_cprr.py data/madisonville_pd/madisonville_pd_cprr_2010-2020.csv
	python clean/madisonville_pd_cprr.py

data/clean/pprr_madisonville_csd_2019.csv: clean/madisonville_csd_pprr.py data/madisonville_csd/madisonville_csd_pprr_2019.csv
	python clean/madisonville_csd_pprr.py
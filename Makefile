current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

data/clean/pprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv
	python clean/new_orleans_harbor_pd.py
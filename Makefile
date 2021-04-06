current_dir = $(shell pwd)
export PYTHONPATH := $(current_dir):$(PYTHONPATH)

.PHONY: all

all: data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv data/fuse/com_new_orleans_harbor_pd.csv
all: data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv data/fuse/app_baton_rouge_pd.csv
all: data/fuse/per_baton_rouge_so.csv data/fuse/perhist_baton_rouge_so.csv data/fuse/com_baton_rouge_so.csv
all: data/fuse/per_brusly_pd.csv data/fuse/perhist_brusly_pd.csv data/fuse/com_brusly_pd.csv
all: data/fuse/per_port_allen_pd.csv data/fuse/perhist_port_allen_pd.csv data/fuse/com_port_allen_pd.csv
all: data/fuse/per_madisonville_pd.csv data/fuse/perhist_madisonville_pd.csv data/fuse/com_madisonville_pd.csv
all: data/fuse/per_greenwood_pd.csv data/fuse/com_greenwood_pd.csv data/fuse/perhist_greenwood_pd.csv
all: data/fuse/com_new_orleans_pd.csv data/fuse/uof_new_orleans_pd.csv data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv
all: data/fuse/per_st_tammany_so.csv data/fuse/perhist_st_tammany_so.csv data/fuse/com_st_tammany_so.csv
all: data/fuse/com_plaquemines_so.csv data/fuse/per_plaquemines_so.csv data/fuse/perhist_plaquemines_so.csv
all: data/fuse/per_louisiana_state_police.csv data/fuse/perhist_louisiana_state_police.csv data/fuse/app_louisiana_state_police.csv

data/fuse/per_new_orleans_harbor_pd.csv data/fuse/perhist_new_orleans_harbor_pd.csv data/fuse/com_new_orleans_harbor_pd.csv: fuse/new_orleans_harbor_pd.py data/match/pprr_new_orleans_harbor_pd_2020.csv data/match/cprr_new_orleans_harbor_pd_2020.csv
	python fuse/new_orleans_harbor_pd.py

data/fuse/per_baton_rouge_pd.csv data/fuse/perhist_baton_rouge_pd.csv data/fuse/com_baton_rouge_pd.csv data/fuse/app_baton_rouge_pd.csv: fuse/baton_rouge_pd.py data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv data/match/cprr_baton_rouge_pd_2018.csv data/match/lprr_baton_rouge_fpcsb_1992_2012.csv
	python fuse/baton_rouge_pd.py

data/fuse/per_baton_rouge_so.csv data/fuse/perhist_baton_rouge_so.csv data/fuse/com_baton_rouge_so.csv: fuse/baton_rouge_so.py data/match/cprr_baton_rouge_so_2018.csv data/clean/pprr_post_2020_11_06.csv
	python fuse/baton_rouge_so.py

data/fuse/per_brusly_pd.csv data/fuse/perhist_brusly_pd.csv data/fuse/com_brusly_pd.csv: fuse/brusly_pd.py data/match/pprr_brusly_pd_2020.csv data/match/cprr_brusly_pd_2020.csv
	python fuse/brusly_pd.py

data/fuse/per_port_allen_pd.csv data/fuse/perhist_port_allen_pd.csv data/fuse/com_port_allen_pd.csv: fuse/port_allen_pd.py data/match/cprr_port_allen_pd_2019.csv data/match/pprr_port_allen_csd_2020.csv
	python fuse/port_allen_pd.py

data/fuse/per_madisonville_pd.csv data/fuse/perhist_madisonville_pd.csv data/fuse/com_madisonville_pd.csv: fuse/madisonville_pd.py data/match/cprr_madisonville_pd_2010_2020.csv data/match/pprr_madisonville_csd_2019.csv
	python fuse/madisonville_pd.py

data/fuse/per_greenwood_pd.csv data/fuse/com_greenwood_pd.csv data/fuse/perhist_greenwood_pd.csv: fuse/greenwood_pd.py data/match/cprr_greenwood_pd_2015_2020.csv data/clean/pprr_post_2020_11_06.csv
	python fuse/greenwood_pd.py

data/fuse/com_new_orleans_pd.csv data/fuse/uof_new_orleans_pd.csv data/fuse/per_new_orleans_pd.csv data/fuse/perhist_new_orleans_pd.csv: fuse/new_orleans_pd.py data/clean/pprr_new_orleans_pd_1946_2018.csv data/clean/cprr_new_orleans_pd_1931_2020.csv data/clean/cprr_actions_new_orleans_pd_1931_2020.csv data/clean/uof_new_orleans_pd_2012_2019.csv
	python fuse/new_orleans_pd.py

data/fuse/per_st_tammany_so.csv data/fuse/perhist_st_tammany_so.csv data/fuse/com_st_tammany_so.csv: fuse/st_tammany_so.py data/match/cprr_st_tammany_so_2011_2021.csv data/match/pprr_st_tammany_so_2020.csv
	python fuse/st_tammany_so.py

data/fuse/com_plaquemines_so.csv data/fuse/per_plaquemines_so.csv data/fuse/perhist_plaquemines_so.csv: fuse/plaquemines_so.py data/match/cprr_plaquemines_so_2019.csv data/clean/pprr_post_2020_11_06.csv
	python fuse/plaquemines_so.py

data/fuse/per_louisiana_state_police.csv data/fuse/perhist_louisiana_state_police.csv data/fuse/app_louisiana_state_police.csv: fuse/louisiana_state_csc.py data/match/lprr_louisiana_state_csc_1991_2020.csv data/clean/pprr_post_2020_11_06.csv
	python fuse/louisiana_state_csc.py



data/match/cprr_new_orleans_harbor_pd_2020.csv data/match/pprr_new_orleans_harbor_pd_2020.csv: match/new_orleans_harbor_pd.py data/clean/cprr_new_orleans_harbor_pd_2020.csv data/clean/pprr_new_orleans_harbor_pd_2020.csv
	python match/new_orleans_harbor_pd.py

data/match/pprr_baton_rouge_csd_2017.csv data/match/pprr_baton_rouge_csd_2019.csv data/match/cprr_baton_rouge_pd_2018.csv data/match/lprr_baton_rouge_fpcsb_1992_2012.csv: match/baton_rouge_pd.py data/clean/pprr_baton_rouge_csd_2017.csv data/clean/pprr_baton_rouge_csd_2019.csv data/clean/cprr_baton_rouge_pd_2018.csv data/clean/pprr_post_2020_11_06.csv data/clean/lprr_baton_rouge_fpcsb_1992_2012.csv
	python match/baton_rouge_pd.py

data/match/cprr_brusly_pd_2020.csv data/match/pprr_brusly_pd_2020.csv: match/brusly_pd.py data/clean/pprr_brusly_pd_2020.csv data/clean/cprr_brusly_pd_2020.csv
	python match/brusly_pd.py

data/match/cprr_madisonville_pd_2010_2020.csv data/match/pprr_madisonville_csd_2019.csv: match/madisonville_pd.py data/clean/cprr_madisonville_pd_2010_2020.csv data/clean/pprr_madisonville_csd_2019.csv data/clean/pprr_post_2020_11_06.csv
	python match/madisonville_pd.py

data/match/cprr_greenwood_pd_2015_2020.csv: match/greenwood_pd.py data/clean/cprr_greenwood_pd_2015_2020.csv data/clean/pprr_post_2020_11_06.csv
	python match/greenwood_pd.py

data/match/pprr_port_allen_csd_2020.csv data/match/cprr_port_allen_pd_2019.csv: match/port_allen_pd.py data/clean/pprr_port_allen_csd_2020.csv data/clean/pprr_post_2020_11_06.csv data/clean/cprr_port_allen_pd_2019.csv
	python match/port_allen_pd.py

data/match/cprr_baton_rouge_so_2018.csv: match/baton_rouge_so.py data/clean/cprr_baton_rouge_so_2018.csv data/clean/pprr_post_2020_11_06.csv
	python match/baton_rouge_so.py

data/match/cprr_baton_rouge_da_2021.csv: match/baton_rouge_da.py data/clean/cprr_baton_rouge_da_2021.csv data/match/pprr_baton_rouge_csd_2019.csv data/match/pprr_baton_rouge_csd_2017.csv data/match/cprr_baton_rouge_so_2018.csv
	python match/baton_rouge_da.py

data/match/cprr_st_tammany_so_2011_2021.csv data/match/pprr_st_tammany_so_2020.csv: match/st_tammany_so.py data/clean/cprr_st_tammany_so_2011_2021.csv data/clean/pprr_st_tammany_so_2020.csv data/clean/pprr_post_2020_11_06.csv
	python match/st_tammany_so.py

data/match/cprr_plaquemines_so_2019.csv: match/plaquemines_so.py data/clean/cprr_plaquemines_so_2019.csv
	python match/plaquemines_so.py

data/match/lprr_louisiana_state_csc_1991_2020.csv: match/louisiana_state_csc.py data/clean/lprr_louisiana_state_csc_1991_2020.csv data/clean/pprr_post_2020_11_06.csv
	python match/louisiana_state_csc.py



data/clean/pprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd_pprr.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_pprr_2020.csv
	python clean/new_orleans_harbor_pd_pprr.py

data/clean/pprr_baton_rouge_csd_2017.csv data/clean/pprr_baton_rouge_csd_2019.csv: clean/baton_rouge_csd_pprr.py data/baton_rouge_csd/*.csv
	python clean/baton_rouge_csd_pprr.py

data/clean/cprr_baton_rouge_pd_2018.csv: clean/baton_rouge_pd_cprr.py data/baton_rouge_pd/baton_rouge_pd_cprr_2018.csv
	python clean/baton_rouge_pd_cprr.py

data/clean/cprr_baton_rouge_so_2018.csv: clean/baton_rouge_so_cprr.py data/baton_rouge_so/baton_rouge_so_cprr_2018.csv
	python clean/baton_rouge_so_cprr.py

data/clean/cprr_new_orleans_harbor_pd_2020.csv: clean/new_orleans_harbor_pd_cprr.py data/new_orleans_harbor_pd/new_orleans_harbor_pd_cprr_2014-2020.csv
	python clean/new_orleans_harbor_pd_cprr.py

data/clean/lprr_louisiana_state_csc_1991_2020.csv: clean/louisiana_state_csc_lprr.py data/louisiana_state_csc/louisianastate_csc_lprr_1991-2020.csv data/louisiana_state_csc/la_lprr_appellants.csv
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

data/clean/pprr_post_2020_11_06.csv: clean/post_pprr.py data/post_council/post_pprr_11-6-2020.csv
	python clean/post_pprr.py

data/clean/cprr_greenwood_pd_2015_2020.csv: clean/greenwood_pd_cprr.py data/greenwood_pd/greenwood_pd_cprr_2015-2020_byhand.csv
	python clean/greenwood_pd_cprr.py

data/clean/pprr_port_allen_csd_2020.csv: clean/port_allen_csd_pprr.py data/port_allen_csd/port_allen_csd_pprr_2020.csv
	python clean/port_allen_csd_pprr.py

data/clean/cprr_baton_rouge_da_2021.csv: clean/baton_rouge_da_cprr_2021.py data/baton_rouge_da/baton_rouge_da_cprr_2021.csv
	python clean/baton_rouge_da_cprr_2021.py

data/clean/cprr_new_orleans_pd_1931_2020.csv: clean/ipm_new_orleans_pd_cprr_allegations.py data/ipm/new_orleans_pd_cprr_allegations_1931-2020.csv
	python clean/ipm_new_orleans_pd_cprr_allegations.py

data/clean/pprr_new_orleans_pd_1946_2018.csv: clean/ipm_new_orleans_iapro_pprr.py data/ipm/new_orleans_pd_pprr_1946-2018.csv
	python clean/ipm_new_orleans_iapro_pprr.py

data/clean/cprr_actions_new_orleans_pd_1931_2020.csv: clean/ipm_new_orleans_pd_cprr_actions_taken.py data/ipm/new_orleans_pd_cprr_actions_taken_1931-2020.csv
	python clean/ipm_new_orleans_pd_cprr_actions_taken.py

data/clean/uof_new_orleans_pd_2012_2019.csv: clean/ipm_new_orleans_pd_use_of_force.py data/ipm/new_orleans_pd_use_of_force_2012-2019.csv
	python clean/ipm_new_orleans_pd_use_of_force.py

data/clean/cprr_st_tammany_so_2011_2021.csv: clean/st_tammany_so_cprr.py data/st_tammany_so/st_tammany_so_cprr_2011-2020_tabula.csv data/st_tammany_so/st_tammany_so_cprr_2020-2021_tabula.csv data/st_tammany_so/st_tammany_department_codes_tabula.csv
	python clean/st_tammany_so_cprr.py

data/clean/pprr_st_tammany_so_2020.csv: clean/st_tammany_so_pprr.py data/st_tammany_so/st._tammany_so_pprr_2020.csv
	python clean/st_tammany_so_pprr.py

data/clean/cprr_plaquemines_so_2019.csv: clean/plaquemines_so_cprr.py data/plaquemines_so/plaquemines_so_cprr_2019.csv
	python clean/plaquemines_so_cprr.py
import pandas as pd
from lib.path import data_file_path
from lib.columns import (
    rearrange_personnel_columns, rearrange_event_columns, rearrange_allegation_columns,
    rearrange_stop_and_search_columns, rearrange_use_of_force
)
from lib.uid import ensure_uid_unique
import sys
sys.path.append("../")


def fuse_personnel():
    return rearrange_personnel_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/per_baton_rouge_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_baton_rouge_so.csv")),
        pd.read_csv(data_file_path("fuse/per_new_orleans_harbor_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_new_orleans_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_brusly_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_port_allen_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_madisonville_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_greenwood_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_st_tammany_so.csv")),
        pd.read_csv(data_file_path("fuse/per_plaquemines_so.csv")),
        pd.read_csv(data_file_path("fuse/per_louisiana_state_police.csv")),
        pd.read_csv(data_file_path("fuse/per_caddo_parish_so.csv")),
        pd.read_csv(data_file_path("fuse/per_mandeville_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_levee_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_grand_isle_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_gretna_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_kenner_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_vivian_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_covington_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_slidell_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_new_orleans_so.csv")),
        pd.read_csv(data_file_path("fuse/per_scott_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_shreveport_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_tangipahoa_so.csv")),
        pd.read_csv(data_file_path("fuse/per_ponchatoula_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_lafayette_so.csv")),
        pd.read_csv(data_file_path("fuse/per_lafayette_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_hammond_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_lake_charles_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_sterlington_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_youngsville_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_west_monroe_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_carencro_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_central_csd.csv")),
        pd.read_csv(data_file_path("fuse/per_bossier_city_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_baker_pd.csv")),
        pd.read_csv(data_file_path("fuse/per_denham_springs_pd.csv")),
    ])).sort_values('uid', ignore_index=True)


def fuse_event():
    return rearrange_event_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/event_baton_rouge_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_baton_rouge_so.csv")),
        pd.read_csv(data_file_path("fuse/event_new_orleans_harbor_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_new_orleans_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_brusly_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_port_allen_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_madisonville_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_greenwood_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_st_tammany_so.csv")),
        pd.read_csv(data_file_path("fuse/event_plaquemines_so.csv")),
        pd.read_csv(data_file_path("fuse/event_louisiana_state_police.csv")),
        pd.read_csv(data_file_path("fuse/event_caddo_parish_so.csv")),
        pd.read_csv(data_file_path("fuse/event_mandeville_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_levee_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_grand_isle_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_gretna_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_kenner_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_vivian_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_covington_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_slidell_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_new_orleans_so.csv")),
        pd.read_csv(data_file_path("fuse/event_scott_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_shreveport_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_tangipahoa_so.csv")),
        pd.read_csv(data_file_path("fuse/event_ponchatoula_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_lafayette_so.csv")),
        pd.read_csv(data_file_path("fuse/event_lafayette_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_hammond_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_lake_charles_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_sterlington_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_youngsville_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_west_monroe_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_carencro_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_central_csd.csv")),
        pd.read_csv(data_file_path("fuse/event_bossier_city_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_baker_pd.csv")),
        pd.read_csv(data_file_path("fuse/event_denham_springs_pd.csv")),
    ])).sort_values(['agency', 'event_uid'], ignore_index=True)


def fuse_allegation():
    return rearrange_allegation_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/com_baton_rouge_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_baton_rouge_so.csv")),
        pd.read_csv(data_file_path("fuse/com_new_orleans_harbor_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_brusly_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_port_allen_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_madisonville_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_greenwood_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_new_orleans_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_st_tammany_so.csv")),
        pd.read_csv(data_file_path("fuse/com_plaquemines_so.csv")),
        pd.read_csv(data_file_path("fuse/com_mandeville_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_levee_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_new_orleans_so.csv")),
        pd.read_csv(data_file_path("fuse/com_scott_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_shreveport_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_tangipahoa_so.csv")),
        pd.read_csv(data_file_path("fuse/com_lafayette_so.csv")),
        pd.read_csv(data_file_path("fuse/com_lafayette_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_hammond_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_ponchatoula_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_lake_charles_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_bossier_city_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_baker_pd.csv")),
        pd.read_csv(data_file_path("fuse/com_denham_springs_pd.csv")),
    ])).sort_values(['agency', 'tracking_number'], ignore_index=True)


def fuse_use_of_force():
    return rearrange_use_of_force(pd.concat([
        pd.read_csv(data_file_path("fuse/uof_new_orleans_pd.csv")),
        pd.read_csv(data_file_path("fuse/uof_kenner_pd.csv")),
    ])).sort_values(['agency', 'uof_tracking_number'])


def fuse_stop_and_search():
    return rearrange_stop_and_search_columns(pd.concat([
        pd.read_csv(data_file_path("fuse/sas_new_orleans_pd.csv"),)
    ])).sort_values(['agency', 'stop_and_search_uid'])


if __name__ == "__main__":
    per_df = fuse_personnel()
    ensure_uid_unique(per_df, 'uid')
    event_df = fuse_event()
    ensure_uid_unique(event_df, 'event_uid')
    allegation_df = fuse_allegation()
    ensure_uid_unique(allegation_df, 'allegation_uid')
    uof_df = fuse_use_of_force()
    ensure_uid_unique(uof_df, 'uof_uid')
    sas_df = fuse_stop_and_search()
    per_df.to_csv(data_file_path("fuse/personnel.csv"), index=False)
    event_df.to_csv(data_file_path("fuse/event.csv"), index=False)
    allegation_df.to_csv(data_file_path("fuse/allegation.csv"), index=False)
    uof_df.to_csv(data_file_path('fuse/use_of_force.csv'), index=False)
    sas_df.to_csv(data_file_path('fuse/stop_and_search.csv'), index=False)

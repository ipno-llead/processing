import pandas as pd
import deba
from lib.columns import (
    rearrange_appeal_hearing_columns,
    rearrange_personnel_columns,
    rearrange_event_columns,
    rearrange_allegation_columns,
    rearrange_stop_and_search_columns,
    rearrange_use_of_force,
    rearrange_award_columns,
    rearrange_uof_citizen_columns,
    rearrange_uof_officer_columns,
)
from lib.uid import ensure_uid_unique


def fuse_personnel():
    return rearrange_personnel_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse/per_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse/per_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse/per_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse/per_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse/per_brusly_pd.csv")),
                pd.read_csv(deba.data("fuse/per_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse/per_madisonville_pd.csv")),
                pd.read_csv(deba.data("fuse/per_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse/per_st_tammany_so.csv")),
                pd.read_csv(deba.data("fuse/per_plaquemines_so.csv")),
                pd.read_csv(deba.data("fuse/per_louisiana_state_police.csv")),
                pd.read_csv(deba.data("fuse/per_caddo_parish_so.csv")),
                pd.read_csv(deba.data("fuse/per_mandeville_pd.csv")),
                pd.read_csv(deba.data("fuse/per_levee_pd.csv")),
                pd.read_csv(deba.data("fuse/per_grand_isle_pd.csv")),
                pd.read_csv(deba.data("fuse/per_gretna_pd.csv")),
                pd.read_csv(deba.data("fuse/per_kenner_pd.csv")),
                pd.read_csv(deba.data("fuse/per_vivian_pd.csv")),
                pd.read_csv(deba.data("fuse/per_covington_pd.csv")),
                pd.read_csv(deba.data("fuse/per_slidell_pd.csv")),
                pd.read_csv(deba.data("fuse/per_new_orleans_so.csv")),
                pd.read_csv(deba.data("fuse/per_scott_pd.csv")),
                pd.read_csv(deba.data("fuse/per_shreveport_pd.csv")),
                pd.read_csv(deba.data("fuse/per_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse/per_ponchatoula_pd.csv")),
                pd.read_csv(deba.data("fuse/per_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse/per_lafayette_pd.csv")),
                pd.read_csv(deba.data("fuse/per_hammond_pd.csv")),
                pd.read_csv(deba.data("fuse/per_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse/per_sterlington_pd.csv")),
                pd.read_csv(deba.data("fuse/per_youngsville_pd.csv")),
                pd.read_csv(deba.data("fuse/per_west_monroe_pd.csv")),
                pd.read_csv(deba.data("fuse/per_carencro_pd.csv")),
                pd.read_csv(deba.data("fuse/per_central_csd.csv")),
                pd.read_csv(deba.data("fuse/per_bossier_city_pd.csv")),
                pd.read_csv(deba.data("fuse/per_baker_pd.csv")),
                pd.read_csv(deba.data("fuse/per_houma_pd.csv")),
                pd.read_csv(deba.data("fuse/per_gonzales_pd.csv")),
                pd.read_csv(deba.data("fuse/per_denham_springs_pd.csv")),
                pd.read_csv(deba.data("fuse/per_abbeville_pd.csv")),
                pd.read_csv(deba.data("fuse/per_washington_so.csv")),
                pd.read_csv(deba.data("fuse/per_cameron_so.csv")),
                pd.read_csv(deba.data("fuse/per_maurice_pd.csv")),
                pd.read_csv(deba.data("fuse/per_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse/per_jefferson_so.csv")),
                pd.read_csv(deba.data("fuse/per_acadia_so.csv")),
                pd.read_csv(deba.data("fuse/per_post.csv")),
                pd.read_csv(deba.data("fuse/per_erath_pd.csv")),
                pd.read_csv(deba.data("fuse/per_st_landry_so.csv")),
                pd.read_csv(deba.data("fuse/per_benton_pd.csv")),
                pd.read_csv(deba.data("fuse/per_eunice_pd.csv")),
                pd.read_csv(deba.data("fuse/per_rayne_pd.csv")),
                pd.read_csv(deba.data("fuse/per_st_john_so.csv")),
                pd.read_csv(deba.data("fuse/per_lafourche_so.csv")),
            ]
        )
    ).sort_values("uid", ignore_index=True)


def fuse_event():
    return rearrange_event_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse/event_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse/event_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse/event_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse/event_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse/event_brusly_pd.csv")),
                pd.read_csv(deba.data("fuse/event_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse/event_madisonville_pd.csv")),
                pd.read_csv(deba.data("fuse/event_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse/event_st_tammany_so.csv")),
                pd.read_csv(deba.data("fuse/event_plaquemines_so.csv")),
                pd.read_csv(deba.data("fuse/event_louisiana_state_police.csv")),
                pd.read_csv(deba.data("fuse/event_caddo_parish_so.csv")),
                pd.read_csv(deba.data("fuse/event_mandeville_pd.csv")),
                pd.read_csv(deba.data("fuse/event_levee_pd.csv")),
                pd.read_csv(deba.data("fuse/event_grand_isle_pd.csv")),
                pd.read_csv(deba.data("fuse/event_gretna_pd.csv")),
                pd.read_csv(deba.data("fuse/event_kenner_pd.csv")),
                pd.read_csv(deba.data("fuse/event_vivian_pd.csv")),
                pd.read_csv(deba.data("fuse/event_covington_pd.csv")),
                pd.read_csv(deba.data("fuse/event_slidell_pd.csv")),
                pd.read_csv(deba.data("fuse/event_new_orleans_so.csv")),
                pd.read_csv(deba.data("fuse/event_scott_pd.csv")),
                pd.read_csv(deba.data("fuse/event_shreveport_pd.csv")),
                pd.read_csv(deba.data("fuse/event_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse/event_ponchatoula_pd.csv")),
                pd.read_csv(deba.data("fuse/event_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse/event_lafayette_pd.csv")),
                pd.read_csv(deba.data("fuse/event_hammond_pd.csv")),
                pd.read_csv(deba.data("fuse/event_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse/event_sterlington_pd.csv")),
                pd.read_csv(deba.data("fuse/event_youngsville_pd.csv")),
                pd.read_csv(deba.data("fuse/event_west_monroe_pd.csv")),
                pd.read_csv(deba.data("fuse/event_carencro_pd.csv")),
                pd.read_csv(deba.data("fuse/event_central_csd.csv")),
                pd.read_csv(deba.data("fuse/event_bossier_city_pd.csv")),
                pd.read_csv(deba.data("fuse/event_baker_pd.csv")),
                pd.read_csv(deba.data("fuse/event_houma_pd.csv")),
                pd.read_csv(deba.data("fuse/event_gonzales_pd.csv")),
                pd.read_csv(deba.data("fuse/event_denham_springs_pd.csv")),
                pd.read_csv(deba.data("fuse/event_abbeville_pd.csv")),
                pd.read_csv(deba.data("fuse/event_washington_so.csv")),
                pd.read_csv(deba.data("fuse/event_cameron_so.csv")),
                pd.read_csv(deba.data("fuse/event_maurice_pd.csv")),
                pd.read_csv(deba.data("fuse/event_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse/event_jefferson_so.csv")),
                pd.read_csv(deba.data("fuse/event_acadia_so.csv")),
                pd.read_csv(deba.data("fuse/event_erath_pd.csv")),
                pd.read_csv(deba.data("fuse/event_st_landry_so.csv")),
                pd.read_csv(deba.data("fuse/event_benton_pd.csv")),
                pd.read_csv(deba.data("fuse/event_eunice_pd.csv")),
                pd.read_csv(deba.data("fuse/event_rayne_pd.csv")),
                pd.read_csv(deba.data("fuse/event_st_john_so.csv")),
                pd.read_csv(deba.data("fuse/event_lafourche_so.csv")),
            ]
        )
    ).sort_values(["agency", "event_uid"], ignore_index=True)


def fuse_allegation():
    return rearrange_allegation_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse/com_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse/com_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse/com_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse/com_brusly_pd.csv")),
                pd.read_csv(deba.data("fuse/com_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse/com_madisonville_pd.csv")),
                pd.read_csv(deba.data("fuse/com_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse/com_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse/com_st_tammany_so.csv")),
                pd.read_csv(deba.data("fuse/com_plaquemines_so.csv")),
                pd.read_csv(deba.data("fuse/com_mandeville_pd.csv")),
                pd.read_csv(deba.data("fuse/com_levee_pd.csv")),
                pd.read_csv(deba.data("fuse/com_new_orleans_so.csv")),
                pd.read_csv(deba.data("fuse/com_scott_pd.csv")),
                pd.read_csv(deba.data("fuse/com_shreveport_pd.csv")),
                pd.read_csv(deba.data("fuse/com_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse/com_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse/com_lafayette_pd.csv")),
                pd.read_csv(deba.data("fuse/com_hammond_pd.csv")),
                pd.read_csv(deba.data("fuse/com_ponchatoula_pd.csv")),
                pd.read_csv(deba.data("fuse/com_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse/com_bossier_city_pd.csv")),
                pd.read_csv(deba.data("fuse/com_baker_pd.csv")),
                pd.read_csv(deba.data("fuse/com_houma_pd.csv")),
                pd.read_csv(deba.data("fuse/com_denham_springs_pd.csv")),
                pd.read_csv(deba.data("fuse/com_abbeville_pd.csv")),
                pd.read_csv(deba.data("fuse/com_washington_so.csv")),
                pd.read_csv(deba.data("fuse/com_cameron_So.csv")),
                pd.read_csv(deba.data("fuse/com_maurice_pd.csv")),
                pd.read_csv(deba.data("fuse/com_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse/com_acadia_so.csv")),
                pd.read_csv(deba.data("fuse/com_rayne_pd.csv")),
                pd.read_csv(deba.data("fuse/com_west_monroe_pd.csv")),
                pd.read_csv(deba.data("fuse/com_erath_pd.csv")),
                pd.read_csv(deba.data("fuse/com_st_landry_so.csv")),
                pd.read_csv(deba.data("fuse/com_benton_pd.csv")),
                pd.read_csv(deba.data("fuse/com_eunice_pd.csv")),
                pd.read_csv(deba.data("fuse/com_st_john_so.csv")),
                pd.read_csv(deba.data("fuse/com_lafourche_so.csv")),
            ]
        )
    ).sort_values(["agency", "tracking_number"], ignore_index=True)


def fuse_use_of_force():
    return rearrange_use_of_force(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse/uof_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse/uof_kenner_pd.csv")),
            ]
        )
    ).sort_values(["agency", "uof_uid"])


def fuse_stop_and_search():
    return rearrange_stop_and_search_columns(
        pd.concat(
            [
                pd.read_csv(
                    deba.data("fuse/sas_new_orleans_pd.csv"),
                )
            ]
        )
    ).sort_values(["agency", "stop_and_search_uid"])


def fuse_uof_citizens():
    return rearrange_uof_citizen_columns(
        pd.concat(
            [
                pd.read_csv(
                    deba.data("fuse/uof_citizens_new_orleans_pd.csv"),
                )
            ]
        )
    ).sort_values(["agency", "uof_citizen_uid"])


def fuse_uof_officers():
    return rearrange_uof_officer_columns(
        pd.concat(
            [
                pd.read_csv(
                    deba.data("fuse/uof_officers_new_orleans_pd.csv"),
                )
            ]
        )
    ).sort_values(["agency", "uid"])


def find_event_agency_if_missing_from_post(event_df, post_event_df):
    missing_event_agency = event_df[~event_df["agency"].isin(post_event_df["agency"])]
    missing_event_agency = missing_event_agency[["agency"]].drop_duplicates().dropna()

    if len(missing_event_agency["agency"]) > 0:
        raise ValueError(
            "Agency not in POST: %s" % missing_event_agency["agency"].tolist()
        )
    return missing_event_agency


def fuse_appeal_hearing_logs():
    return rearrange_appeal_hearing_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse/app_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse/app_new_orleans_csc.csv")),
                pd.read_csv(deba.data("fuse/app_louisiana_state_police.csv")),
            ]
        )
    ).sort_values("uid", ignore_index=True)


def fuse_award():
    return rearrange_award_columns(
        pd.concat(
            [
                pd.read_csv(
                    deba.data("fuse/award_lafayette_so.csv"),
                )
            ]
        )
    ).sort_values(["agency", "award_uid"])


if __name__ == "__main__":
    per_df = fuse_personnel()
    ensure_uid_unique(per_df, "uid")
    event_df = fuse_event()
    ensure_uid_unique(event_df, "event_uid")
    allegation_df = fuse_allegation()
    ensure_uid_unique(allegation_df, "allegation_uid")
    uof_df = fuse_use_of_force()
    ensure_uid_unique(uof_df, "uof_uid")
    sas_df = fuse_stop_and_search()
    app_df = fuse_appeal_hearing_logs()
<<<<<<< HEAD
    award_df = fuse_award()
=======
    uof_citizen_df = fuse_uof_citizens()
    uof_officer_df = fuse_uof_officers()
>>>>>>> origin
    per_df.to_csv(deba.data("fuse/personnel.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse/allegation.csv"), index=False)
    uof_df.to_csv(deba.data("fuse/use_of_force.csv"), index=False)
    sas_df.to_csv(deba.data("fuse/stop_and_search.csv"), index=False)
    app_df.to_csv(deba.data("fuse/appeals.csv"), index=False)
<<<<<<< HEAD
    award_df.to_csv(deba.data("fuse/award.csv"), index=False)
=======
    uof_citizen_df.to_csv(deba.data("fuse/uof_citizens.csv"), index=False)
    uof_officer_df.to_csv(deba.data("fuse/uof_officers.csv"), index=False)
>>>>>>> origin

    post_event_df = pd.read_csv(deba.data("fuse/events_post.csv"))
    missing_agency_df = find_event_agency_if_missing_from_post(event_df, post_event_df)
    post_event_df = post_event_df[~post_event_df["agency"].isin(event_df["agency"])]
    event_df = pd.concat([event_df, post_event_df], ignore_index=True)
    event_df.to_csv(deba.data("fuse/event.csv"), index=False)

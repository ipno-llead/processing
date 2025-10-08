import pandas as pd
import deba
from lib.columns import (
    rearrange_appeal_hearing_columns,
    rearrange_personnel_columns,
    rearrange_event_columns,
    rearrange_allegation_columns,
    rearrange_property_claims_columns,
    rearrange_stop_and_search_columns,
    rearrange_use_of_force,
    rearrange_award_columns,
    rearrange_brady_columns,
    rearrange_settlement_columns,
    rearrange_docs_columns,
    rearrange_police_report_columns,
    rearrange_citizen_columns,
    rearrange_agency_columns
)
from lib.uid import ensure_uid_unique
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex, DissimilarFilter


def read_event():
    return rearrange_event_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/event_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_brusly_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_madisonville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_st_tammany_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_plaquemines_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_louisiana_state_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_caddo_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_mandeville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_levee_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_grand_isle_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_gretna_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_kenner_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_vivian_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_covington_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_slidell_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_new_orleans_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_scott_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_shreveport_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_ponchatoula_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_lafayette_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_hammond_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_sterlington_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_youngsville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_west_monroe_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_carencro_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_central_csd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_bossier_city_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_baker_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_houma_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_gonzales_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_denham_springs_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_abbeville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_washington_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_cameron_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_maurice_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_jefferson_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_acadia_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_erath_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_st_landry_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_benton_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_eunice_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_rayne_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_st_john_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_lafourche_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_ascension_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_sulphur_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_pineville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_st_james_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_natchitoches_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_harahan_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_ouachita_da.csv")),
                pd.read_csv(deba.data("fuse_agency/event_baton_rouge_da.csv")),
                pd.read_csv(deba.data("fuse_agency/event_morehouse_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_iberia_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_lockport_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/event_jefferson_davis_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_morehouse_da.csv")),
                pd.read_csv(deba.data("fuse_agency/event_east_feliciana_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_lasalle_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_point_coupee_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_richland_so.csv")),
                pd.read_csv(deba.data("fuse_agency/event_west_feliciana_so.csv")),
            ]
        )
    ).sort_values(["agency", "event_uid"], ignore_index=True)


def fuse_allegation():
    return rearrange_allegation_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/com_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_brusly_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_madisonville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_st_tammany_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_plaquemines_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_mandeville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_levee_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_new_orleans_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_scott_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_shreveport_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_lafayette_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_hammond_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_ponchatoula_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_bossier_city_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_baker_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_houma_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_denham_springs_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_abbeville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_washington_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_cameron_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_maurice_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_acadia_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_rayne_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_west_monroe_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_erath_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_st_landry_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_benton_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_eunice_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_st_john_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_lafourche_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_ascension_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_sulphur_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_pineville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_st_james_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_natchitoches_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_louisiana_state_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_morehouse_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_iberia_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_lockport_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/com_jefferson_davis_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_post.csv")),
                pd.read_csv(deba.data("fuse_agency/com_east_feliciana_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_lasalle_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_point_coupee_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_richland_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_caddo_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_west_feliciana_so.csv")),
                pd.read_csv(deba.data("fuse_agency/com_slidell_pd.csv")),
            ]
        )
    ).sort_values(["agency", "tracking_id"], ignore_index=True)


def fuse_use_of_force():
    return rearrange_use_of_force(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/uof_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/uof_kenner_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/uof_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse_agency/uof_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse_agency/uof_levee_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/uof_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/uof_baton_rouge_so.csv")),
            ]
        )
    ).sort_values(["agency", "uof_uid"])


def fuse_stop_and_search():
    return rearrange_stop_and_search_columns(
        pd.concat(
            [
                pd.read_csv(
                    deba.data("fuse_agency/sas_new_orleans_pd.csv"),
                ),
                pd.read_csv(
                    deba.data("fuse_agency/sas_baton_rouge_so.csv"),
                )
            ]
        )
    ).sort_values(["agency", "stop_and_search_uid"])


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
                pd.read_csv(deba.data("fuse_agency/app_new_orleans_csc.csv")),
                pd.read_csv(deba.data("fuse_agency/app_louisiana_state_pd.csv")),
            ]
        )
    ).sort_values("uid", ignore_index=True)


def fuse_award():
    return rearrange_award_columns(
        pd.concat(
            [
                pd.read_csv(
                    deba.data("fuse_agency/award_lafayette_so.csv"),
                )
            ]
        )
    ).sort_values(["agency", "award_uid"])


def fuse_brady():
    return rearrange_brady_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/brady_baton_rouge_da.csv")),
                pd.read_csv(deba.data("fuse_agency/brady_ouachita_da.csv")),
                pd.read_csv(deba.data("fuse_agency/brady_morehouse_da.csv")),
            ]
        )
    ).sort_values("brady_uid", ignore_index=True)


def fuse_property_claims():
    return rearrange_property_claims_columns(
        pd.concat([pd.read_csv(deba.data("fuse_agency/pclaims_new_orleans_pd.csv"))])
    ).sort_values("property_claims_uid", ignore_index=True)


def fuse_settlements():
    return rearrange_settlement_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/settlements_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/settlements_louisiana_state_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/settlements_baton_rouge_pd.csv")),
            ]
        )
    ).sort_values("settlement_uid", ignore_index=True)


def fuse_docs():
    return rearrange_docs_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/docs_louisiana_state_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/docs_budgets.csv")),
            ]
        )
    ).sort_values("agency", ignore_index=True)


def fuse_police_reports():
    return rearrange_police_report_columns(
        pd.concat([pd.read_csv(deba.data("fuse_agency/pr_new_orleans_pd.csv"))])
    ).sort_values("agency", ignore_index=True)


def fuse_citizen_dfs():
    return rearrange_citizen_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/cit_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_levee_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_sulphur_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_washington_so.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse_agency/cit_kenner_pd.csv")),
            ]
        )
    ).sort_values("agency", ignore_index=True)


def fuse_agency_lists():
    return rearrange_agency_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("clean/agency_reference_list.csv")),
            ]
        )
    ).sort_values("agency_name", ignore_index=True)


def match_per_with_post(per, post, events, allegation_df, uof_df, sas_df, app_df, brady_df, property_claims_df, settlements, police_reports ):
    dfa = (
        per.loc[per.uid.notna(), ["uid", "first_name", "last_name", "agency"]]
        .drop_duplicates(subset=["uid"])
    )
    dfa.loc[:, "dissimilar_filter"] = dfa.uid
    dfa = dfa.set_index("uid", drop=True)
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:5])
    dfa.loc[:, "lc"] = dfa.last_name.fillna("").map(lambda x: x[:5])

    dfb = (
        post[["uid", "first_name", "last_name", "agency"]]
        .drop_duplicates()
    )
    dfb.loc[:, "dissimilar_filter"] = dfb.uid
    dfb = dfb.set_index("uid", drop=True)
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:5])
    dfb.loc[:, "lc"] = dfb.last_name.fillna("").map(lambda x: x[:5])

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc", "lc", "agency"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
        filters=[
            DissimilarFilter("dissimilar_filter"),
        ],
    )
    decision = .990
    matcher.save_pairs_to_excel(
        deba.data("fuse/personnel.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)
    match_list = list(match_dict.items())
    match_df = pd.DataFrame(match_list, columns=['per', 'post'])
    match_df.to_csv("match_dict.csv")
    match_df.set_index('per', inplace=True)
    match_dict = match_df['post'].to_dict()

    per['uid'] = per['uid'].map(match_dict).fillna(per['uid'])
    events['uid'] = events['uid'].map(match_dict).fillna(events['uid'])
    allegation_df['uid'] = allegation_df['uid'].map(match_dict).fillna(allegation_df['uid'])
    uof_df['uid'] = uof_df['uid'].map(match_dict).fillna(uof_df['uid'])
    sas_df['uid'] = sas_df['uid'].map(match_dict).fillna(sas_df['uid'])
    app_df['uid'] = app_df['uid'].map(match_dict).fillna(app_df['uid'])
    brady_df['uid'] = brady_df['uid'].map(match_dict).fillna(brady_df['uid'])
    property_claims_df['uid'] = property_claims_df['uid'].map(match_dict).fillna(property_claims_df['uid'])
    settlements['uid'] = settlements['uid'].map(match_dict).fillna(settlements['uid'])
    police_reports['uid'] = police_reports['uid'].map(match_dict).fillna(police_reports['uid'])

    return per, post, events, allegation_df, uof_df, sas_df, app_df, brady_df, property_claims_df, settlements, police_reports 


def read_personnel():
    return rearrange_personnel_columns(
        pd.concat(
            [
                pd.read_csv(deba.data("fuse_agency/per_baton_rouge_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_baton_rouge_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_new_orleans_harbor_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_new_orleans_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_brusly_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_port_allen_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_madisonville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_greenwood_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_st_tammany_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_plaquemines_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_louisiana_state_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_caddo_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_mandeville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_levee_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_grand_isle_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_gretna_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_kenner_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_vivian_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_covington_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_slidell_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_new_orleans_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_scott_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_shreveport_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_tangipahoa_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_ponchatoula_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_lafayette_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_lafayette_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_hammond_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_lake_charles_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_sterlington_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_youngsville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_west_monroe_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_carencro_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_central_csd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_bossier_city_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_baker_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_houma_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_gonzales_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_denham_springs_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_abbeville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_washington_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_cameron_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_maurice_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_terrebonne_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_jefferson_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_acadia_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_erath_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_st_landry_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_benton_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_eunice_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_rayne_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_st_john_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_lafourche_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_ascension_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_sulphur_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_pineville_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_st_james_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_natchitoches_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_harahan_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_morehouse_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_iberia_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_lockport_pd.csv")),
                pd.read_csv(deba.data("fuse_agency/per_jefferson_davis_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_baton_rouge_da.csv")),
                pd.read_csv(deba.data("fuse_agency/per_ouachita_da.csv")),
                pd.read_csv(deba.data("fuse_agency/per_morehouse_da.csv")),
                pd.read_csv(deba.data("fuse_agency/per_east_feliciana_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_lasalle_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_point_coupee_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_richland_so.csv")),
                pd.read_csv(deba.data("fuse_agency/per_west_feliciana_so.csv")),
            ]
        )
    ).sort_values("uid", ignore_index=True)


def read_post():
    df = pd.read_csv(deba.data("fuse_agency/per_post.csv"))
    return df 


def fuse_personnel(per_dfs, post):
    return rearrange_personnel_columns(
        pd.concat(
            [
                per_dfs,
                post
            ]
        )
    ).sort_values("uid", ignore_index=True)


if __name__ == "__main__":
    events = read_event()


    allegation_df = fuse_allegation()
    ensure_uid_unique(allegation_df, "allegation_uid")
    uof_df = fuse_use_of_force()
    ensure_uid_unique(uof_df, "uof_uid")
    sas_df = fuse_stop_and_search()
    app_df = fuse_appeal_hearing_logs()
    award_df = fuse_award()
    brady_df = fuse_brady()
    property_claims_df = fuse_property_claims()
    settlements = fuse_settlements()
    # docs = fuse_docs()
    police_reports = fuse_police_reports()
    citizens = fuse_citizen_dfs()
    agencies = fuse_agency_lists()

    per = read_personnel()
    post = read_post()
    per_df, post, event_df, allegation_df, uof_df, sas_df, app_df, brady_df, property_claims_df, settlements, police_reports = match_per_with_post(per, post, events, allegation_df, uof_df, sas_df, app_df, brady_df, property_claims_df, settlements, police_reports)
    per_df = fuse_personnel(per_df, post)

    ensure_uid_unique(per_df, "uid")
    ensure_uid_unique(event_df, "event_uid")


    event_df.to_csv("events.csv", index=False)

    per_df.to_csv(deba.data("fuse_agency/personnel_pre_post.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse_agency/allegation.csv"), index=False)
    uof_df.to_csv(deba.data("fuse_agency/use_of_force.csv"), index=False)
    sas_df.to_csv(deba.data("fuse_agency/stop_and_search.csv"), index=False)
    app_df.to_csv(deba.data("fuse_agency/appeals.csv"), index=False)
    award_df.to_csv(deba.data("fuse_agency/awards.csv"), index=False)
    brady_df.to_csv(deba.data("fuse_agency/brady.csv"), index=False)
    settlements.to_csv(deba.data("fuse_agency/settlements.csv"), index=False)
    # docs.to_csv(deba.data("fuse_agency/docs.csv"), index=False)

    post_event_df = pd.read_csv(deba.data("fuse_agency/event_post.csv"))
    missing_agency_df = find_event_agency_if_missing_from_post(event_df, post_event_df)
    event_df = pd.concat([event_df, post_event_df], ignore_index=True)
    event_df.to_csv(deba.data("fuse_agency/event_pre_post.csv"), index=False)
    property_claims_df.to_csv(deba.data("fuse_agency/property_claims.csv"), index=False)
    police_reports.to_csv(deba.data("fuse_agency/police_reports.csv"), index=False)
    citizens.to_csv(deba.data("fuse_agency/citizens.csv"), index=False)
    agencies.to_csv(deba.data("fuse_agency/agency_reference_list.csv"), index=False)

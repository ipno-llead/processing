from lib.date import combine_date_columns
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, NoopIndex, DateSimilarity
import deba
from lib.post import extract_events_from_post, load_for_agency
import pandas as pd


def match_csd_pprr_against_post_pprr(pprr, post):
    dfa = pprr[["uid", "first_name", "last_name"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa = dfa.set_index("uid")

    dfb = post[["uid", "first_name", "last_name"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb = dfb.set_index("uid")

    matcher = ThresholdMatcher(
        NoopIndex(),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.989
    matcher.save_pairs_to_excel(
        deba.data("match/port_allen_csd_pprr_2020_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    return extract_events_from_post(post, matches, "port-allen-pd")


def match_cprr_against_csd_pprr_2020(cprr, pprr, year, decision):
    dfa = cprr[["first_name", "last_name", "uid"]].drop_duplicates().set_index("uid")

    dfb = pprr[["first_name", "last_name", "uid"]].set_index("uid")

    matcher = ThresholdMatcher(
        NoopIndex(),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    matcher.save_pairs_to_excel(
        deba.data("match/port_allen_pd_cprr_%d_v_csd_pprr_2020.xlsx" % year),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


if __name__ == "__main__":
    pprr = pd.read_csv(deba.data("clean/pprr_port_allen_csd_2020.csv"))
    cprr19 = pd.read_csv(deba.data("clean/cprr_port_allen_pd_2019.csv"))
    cprr18 = pd.read_csv(deba.data("clean/cprr_port_allen_pd_2017_2018.csv"))
    cprr16 = pd.read_csv(deba.data("clean/cprr_port_allen_pd_2015_2016.csv"))
    agency = cprr16.agency[0]
    post = load_for_agency(agency)
    post_event = match_csd_pprr_against_post_pprr(pprr, post)
    cprr19 = match_cprr_against_csd_pprr_2020(cprr19, pprr, 2019, 0.96)
    cprr18 = match_cprr_against_csd_pprr_2020(cprr18, pprr, 2018, 1)
    cprr16 = match_cprr_against_csd_pprr_2020(cprr16, pprr, 2016, 1)

    pprr.to_csv(deba.data("match/pprr_port_allen_csd_2020.csv"), index=False)
    cprr19.to_csv(deba.data("match/cprr_port_allen_pd_2019.csv"), index=False)
    cprr18.to_csv(deba.data("match/cprr_port_allen_pd_2017_2018.csv"), index=False)
    cprr16.to_csv(deba.data("match/cprr_port_allen_pd_2015_2016.csv"), index=False)
    post_event.to_csv(deba.data("match/post_event_port_allen_pd.csv"), index=False)

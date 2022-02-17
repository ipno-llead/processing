import pandas as pd
from datamatch import (
    ThresholdMatcher,
    JaroWinklerSimilarity,
    ColumnsIndex,
    DateSimilarity,
)

import dirk
from lib.date import combine_date_columns
from lib.post import extract_events_from_post, load_for_agency


def extract_post_events(pprr, post):
    dfa = pprr[["first_name", "last_name", "uid"]]
    dfa.loc[:, "hire_date"] = combine_date_columns(
        pprr, "hire_year", "hire_month", "hire_day"
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfa = dfa.drop_duplicates().set_index("uid", drop=True)

    dfb = post[["last_name", "first_name", "uid"]]
    dfb.loc[:, "hire_date"] = combine_date_columns(
        post, "hire_year", "hire_month", "hire_day"
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid", drop=True)

    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "hire_date": DateSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.817
    matcher.save_pairs_to_excel(
        dirk.data("match/gretna_pd_pprr_2018_v_post_pprr_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(lower_bound=decision)

    return extract_events_from_post(post, matches, "Gretna PD")


if __name__ == "__main__":
    pprr = pd.read_csv(dirk.data("clean/pprr_gretna_pd_2018.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    post_events = extract_post_events(pprr, post)

    post_events.to_csv(dirk.data("match/post_event_gretna_pd_2020.csv"), index=False)

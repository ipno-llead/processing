import pandas as pd
import deba
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.columns import rearrange_allegation_columns
from lib.personnel import fuse_personnel, rearrange_personnel_columns
from lib.post import load_for_agency
from lib import events


def match_cprr_and_post(cprr, post):
    dfa = cprr[["uid", "first_name", "last_name", "agency"]]
    dfa = dfa.drop_duplicates(subset=["uid"]).set_index("uid")
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])

    dfb = post[["uid", "first_name", "last_name", "agency"]]
    dfb = dfb.drop_duplicates(subset=["uid"]).set_index("uid")
    dfb.loc[:, "fc"] = dfb.first_name.map(lambda x: x[:1])

    matcher = ThresholdMatcher(
        ColumnsIndex("agency"),
        {
            "first_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = 0.903
    matcher.save_pairs_to_excel(
        deba.data("match_history/cprr_the_advocate_v_post_2020_11_06.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)

    cprr.loc[:, "uid"] = cprr.uid.map(lambda x: match_dict.get(x, x))
    return cprr


def fuse_events(cprr, post):
    builder = events.Builder()
    builder.extract_events(
        cprr,
        {
            events.INVESTIGATION_COMPLETE: {
                "prefix": "investigation_complete",
                "parse_date": True,
                "keep": ["uid", "agency", "allegation", "action", "disposition"],
            }
        },
        ["uid", "allegation_uid"],
    )
    builder.extract_events(
        post,
        {
            events.OFFICER_LEVEL_1_CERT: {
                "prefix": "level_1_cert",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency"],
            },
            events.OFFICER_PC_12_QUALIFICATION: {
                "prefix": "last_pc_12_qualification",
                "parse_date": "%Y-%m-%d",
                "keep": ["uid", "agency"],
            },
            events.OFFICER_HIRE: {"prefix": "hire", "keep": ["uid", "agency"]},
        },
        ["uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("clean/the_advocate_convictions.csv"))
    agency = cprr.agency[0]
    post = load_for_agency(agency)
    cprr = match_cprr_and_post(cprr, post)

    per = rearrange_personnel_columns(cprr)
    per = fuse_personnel(per, post)
    complaints = rearrange_allegation_columns(cprr)
    event = fuse_events(cprr, post)
    complaints.to_csv(deba.data("match_history/com_the_advocate.csv"), index=False)
    per.to_csv(deba.data("match_history/per_the_advocate.csv"), index=False)
    event.to_csv(deba.data("match_history/event_the_advocate.csv"), index=False)

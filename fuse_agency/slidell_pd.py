import pandas as pd

import deba
from lib.columns import rearrange_event_columns, rearrange_allegation_columns
from lib.personnel import fuse_personnel
from lib import events
from lib.post import load_for_agency


def fuse_events(pprr,cprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "parse_date": True,
                "keep": [
                    "uid",
                    "agency",
                    "department_desc",
                    "badge_no",
                    "employment_status",
                ],
                "merge_cols": ["department_desc", "badge_no", "employment_status"],
            },
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "salary",
                "parse_date": True,
                "keep": ["uid", "agency", "rank_desc", "salary", "salary_freq"],
            },
            events.OFFICER_LEFT: {
                "prefix": "termination",
                "parse_date": True,
                "keep": ["uid", "agency"],
            },
        },
        ["uid"],
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_RECEIVE: {
                "prefix": "receive",
                "keep": ["agency", "allegation_uid", "uid"],
            }
        },
        ["allegation_uid"],
    )
    )
    return builder.to_frame(output_duplicated_events=True)


if __name__ == "__main__":
    pprr_csd = pd.read_csv(deba.data("clean/pprr_slidell_csd_2010_2019.csv"))
    cprr = pd.read_csv(deba.data("match/cprr_slidell_pd_2007_2010.csv"))
    agency = pprr_csd.agency[0]
    post = load_for_agency(agency)
    post_event = pd.read_csv(deba.data("match/post_event_slidell_pd_2020.csv"))
    events_df = rearrange_event_columns(pd.concat([post_event, fuse_events(pprr_csd, cprr)]))
    per_df = fuse_personnel(pprr_csd, post, cprr)
    com = rearrange_allegation_columns(cprr)
    per_df.to_csv(deba.data("fuse_agency/per_slidell_pd.csv"), index=False)
    events_df.to_csv(deba.data("fuse_agency/event_slidell_pd.csv"), index=False)
    com.to_csv(deba.data("fuse_agency/com_slidell_pd.csv"), index=False)
    post.to_csv(deba.data("fuse_agency/post_slidell_pd.csv"), index=False)

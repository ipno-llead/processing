import pandas as pd
import deba
from lib.columns import (
    rearrange_allegation_columns,
    rearrange_personnel_columns,
    rearrange_event_columns,
)
from lib import events
from lib.personnel import fuse_personnel
from lib.post import load_for_agency


def fuse_events(pprr, cprr):
    builder = events.Builder()
    builder.extract_events(
        pprr,
        {
            events.OFFICER_HIRE: {
                "prefix": "hire",
                "keep": ["uid", "agency", "badge_no"],
            },
            events.OFFICER_PAY_EFFECTIVE: {
                "prefix": "pay_effective",
                "keep": ["uid", "agency", "badge_no", "salary", "salary_freq"],
            },
        },
        ["uid"],
    )
    builder.extract_events(
        cprr,
        {
            events.COMPLAINT_INCIDENT: {
                "prefix": "incident",
                "keep": ["uid", "agency", "allegation_uid"],
            }
        },
        ["uid", "allegation_uid"],
    )
    return builder.to_frame()


if __name__ == "__main__":
    cprr = pd.read_csv(deba.data("match/cprr_madisonville_pd_2010_2020.csv"))
    pprr = pd.read_csv(deba.data("clean/pprr_madisonville_csd_2019.csv"))
    agency = pprr.agency[0]
    post = load_for_agency(agency)
    pprr.loc[:, "agency"] = "madisonville-pd"
    post_event = pd.read_csv(deba.data("match/post_event_madisonville_csd_2019.csv"))
    per = rearrange_personnel_columns(pprr)
    per = fuse_personnel(per, post)
    com = rearrange_allegation_columns(cprr)
    event = fuse_events(pprr, cprr)
    event = rearrange_event_columns(pd.concat([post_event, event]))
    per.to_csv(deba.data("fuse/per_madisonville_pd.csv"), index=False)
    event.to_csv(deba.data("fuse/event_madisonville_pd.csv"), index=False)
    com.to_csv(deba.data("fuse/com_madisonville_pd.csv"), index=False)
    post.to_csv(deba.data("fuse/post_madisonville_pd.csv"), index=False)

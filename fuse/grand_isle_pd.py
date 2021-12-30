import pandas as pd
from lib.path import data_file_path
from lib.columns import rearrange_personnel_columns, rearrange_event_columns
from lib import events

import sys

sys.path.append("../")


def fuse_events(pprr):
    builder = events.Builder()
    # cannot use extract_events here because logic is more complex
    for _, row in pprr.iterrows():
        if pd.notnull(row.hire_year):
            kwargs = {
                "year": row.hire_year,
                "month": row.hire_month,
                "day": row.hire_day,
                "uid": row.uid,
                "agency": row.agency,
                "employment_status": row.employment_status,
            }
            if pd.isnull(row.pay_effective_year):
                kwargs["salary"] = row.salary
                kwargs["salary_freq"] = row.salary_freq
            builder.append_record(events.OFFICER_HIRE, ["uid"], **kwargs)
        if pd.notnull(row.pay_effective_year):
            kwargs = {
                "year": row.pay_effective_year,
                "month": row.pay_effective_month,
                "day": row.pay_effective_day,
                "uid": row.uid,
                "agency": row.agency,
                "salary": row.salary,
                "salary_freq": row.salary_freq,
            }
            builder.append_record(events.OFFICER_PAY_EFFECTIVE, ["uid"], **kwargs)
    return builder.to_frame()


if __name__ == "__main__":
    pprr = pd.read_csv(data_file_path("clean/pprr_grand_isle_pd_2021.csv"))
    post_event = pd.read_csv(data_file_path("match/post_event_grand_isle_pd.csv"))
    events_df = fuse_events(pprr)
    events_df = rearrange_event_columns(pd.concat([post_event, events_df]))
    rearrange_personnel_columns(pprr).to_csv(data_file_path("fuse/per_grand_isle_pd.csv"), index=False)
    events_df.to_csv(data_file_path("fuse/event_grand_isle_pd.csv"), index=False)

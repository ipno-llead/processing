from lib import events
import pandas as pd
from lib.path import data_file_path
import sys

sys.path.append("../")


def keep_latest_row_for_each_post_officer(post: pd.DataFrame) -> pd.DataFrame:
    """Sort and discard all but the latest rows for each officer in POST data"""
    duplicated_uids = set(post.loc[post.uid.duplicated(), "uid"].to_list())
    post = post.set_index("uid", drop=False)
    level_1_cert_dates = post.loc[
        post.uid.isin(duplicated_uids) & (post.level_1_cert_date.notna()),
        "level_1_cert_date",
    ]
    for idx, value in level_1_cert_dates.iteritems():
        post.loc[idx, "level_1_cert_date"] = value
    post = post.sort_values("last_pc_12_qualification_date", ascending=False)
    return post[~post.index.duplicated(keep="first")]


def extract_events_from_post(
    post: pd.DataFrame, uid_matches: list[tuple[str, str]], agency: str
) -> pd.DataFrame:
    """Extract events from POST data.

    Only extract events.OFFICER_LEVEL_1_CERT and events.OFFICER_PC_12_QUALIFICATION

    Args:
        post (pd.DataFrame):
            the subset of POST data already filtered for a specific agency
        uid_matches (list of tuple of str):
            list of (pprr_uid, post_uid) tuples. Only rows with uid==post_uid
            are considered for event extraction. uid will be set to pprr_uid
            in event records.
        agency (str):
            agency name as it should appear in event records.

    Returns:
        the events frame
    """
    builder = events.Builder()
    for pprr_uid, post_uid in uid_matches:
        for _, row in post[post.uid == post_uid].iterrows():
            if pd.notnull(row.level_1_cert_date):
                builder.append_record(
                    events.OFFICER_LEVEL_1_CERT,
                    ["uid"],
                    raw_date_str=row.level_1_cert_date,
                    strptime_format="%Y-%m-%d",
                    agency=agency,
                    uid=pprr_uid,
                )
            if pd.notnull(row.last_pc_12_qualification_date):
                builder.append_record(
                    events.OFFICER_PC_12_QUALIFICATION,
                    ["uid"],
                    raw_date_str=row.last_pc_12_qualification_date,
                    strptime_format="%Y-%m-%d",
                    agency=agency,
                    uid=pprr_uid,
                )
    return builder.to_frame()


def extract_events_from_cprr_post(
    cprr_post: pd.DataFrame, uid_matches: list[tuple[str, str]], agency: str
) -> pd.DataFrame:
    """Extract events from POST data.

    Only extract events.OFFICER_POST_DECERTIFICATION

    Args:
        post (pd.DataFrame):
            the subset of POST data already filtered for a specific agency
        uid_matches (list of tuple of str):
            list of (pprr_uid, post_uid) tuples. Only rows with uid==post_uid
            are considered for event extraction. uid will be set to pprr_uid
            in event records.
        agency (str):
            agency name as it should appear in event records.

    Returns:
        the events frame
    """
    builder = events.Builder()
    for pprr_uid, cprr_post_uid in uid_matches:
        for _, row in cprr_post[cprr_post.uid == cprr_post_uid].iterrows():
            if pd.notnull(row.decertification_date):
                builder.append_record(
                    events.OFFICER_POST_DECERTIFICATION,
                    ["uid"],
                    raw_date_str=row.decertification_date,
                    agency=agency,
                    uid=pprr_uid,
                )
    return builder.to_frame()


def load_for_agency(agency):
    post = pd.read_csv(data_file_path("clean/pprr_post_2020_11_06.csv"))
    post = post.loc[post.agency == agency]
    if len(post) == 0:
        raise ValueError("agency not found", agency)
    return post

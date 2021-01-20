import pandas as pd
from lib.path import data_file_path, ensure_data_dir
from lib.clean import float_to_int_str
from lib.columns import (
    rearrange_personel_columns, rearrange_personel_history_columns, rearrange_complaint_columns
)

import sys
sys.path.append("../")


def fuse_cprr_baton_rouge_pd_2018():
    df = pd.read_csv(
        data_file_path("clean/cprr_baton_rouge_pd_2018.csv"))
    return (
        rearrange_personel_columns(df),
        rearrange_personel_history_columns(df),
        rearrange_complaint_columns(df)
    )


def fuse_pprr_new_orleans_pd():
    df = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_pd.csv"))
    return (
        rearrange_personel_columns(df),
        rearrange_personel_history_columns(df),
        None
    )


def fuse_pprr_new_orleans_harbor_pd_2020():
    df = pd.read_csv(
        data_file_path("clean/pprr_new_orleans_harbor_pd_2020.csv"))
    return (
        rearrange_personel_columns(df),
        rearrange_personel_history_columns(df),
        None
    )


def fuse_all():
    personel_dfs = []
    history_dfs = []
    complaint_dfs = []
    for fn in [
            fuse_cprr_baton_rouge_pd_2018,
            fuse_pprr_new_orleans_pd,
            fuse_pprr_new_orleans_harbor_pd_2020]:
        personel_df, history_df, complaint_df = fn()
        if personel_df is not None:
            personel_dfs.append(personel_df)
        if history_df is not None:
            history_dfs.append(history_df)
        if complaint_df is not None:
            complaint_dfs.append(complaint_df)
    personel_df = pd.concat(personel_dfs)
    history_df = pd.concat(history_dfs)
    complaint_df = pd.concat(complaint_dfs)
    personel_df = float_to_int_str(
        personel_df, ["employee_id", "birth_year", "birth_month", "birth_day"])
    history_df = float_to_int_str(history_df, [
        "badge_no",
        "rank_year",
        "rank_month",
        "rank_day",
        "hire_year",
        "hire_month",
        "hire_day",
        "term_year",
        "term_month",
        "term_day",
        "pay_prog_start_year",
        "pay_prog_start_month",
        "pay_prog_start_day",
        "pay_effective_year",
        "pay_effective_month",
        "pay_effective_day",
        "data_production_year"
    ])
    complaint_df = float_to_int_str(complaint_df, [
        "occur_year",
        "occur_month",
        "occur_day",
        "receive_year",
        "receive_month",
        "receive_day",
        "investigation_complete_year",
        "investigation_complete_month",
        "investigation_complete_day",
        "paragraph_code"
    ])
    return personel_df, history_df, complaint_df


if __name__ == "__main__":
    personel_df, history_df, complaint_df = fuse_all()
    ensure_data_dir("fuse")
    personel_df.to_csv(data_file_path("fuse/personel.csv"), index=False)
    history_df.to_csv(data_file_path("fuse/personel_history.csv"), index=False)
    complaint_df.to_csv(data_file_path("fuse/complaint.csv"), index=False)

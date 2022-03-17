from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import deba
from lib.clean import standardize_desc_cols, clean_ranks
import pandas as pd


def extract_rank(df):
    ranks = (
        df.nominee.str.lower()
        .str.strip()
        .str.replace("\.", "", regex=False)
        .str.replace("sgt", "sergeant", regex=False)
        .str.extract(
            r"(\(\w+/?\w+\)|special agent|atf agent|agent|detective|deputy|investigative analyst|"
            r"mag unit crime analyst|task force officer|retired detective|supervisory special agent|"
            r"group supervisor|supervisory special agent|s/t|tpr)"
        )
    )
    df.loc[:, "rank_desc"] = ranks[0]\
        .str.replace(r"s\/t", "", regex=True)\
        .str.replace(r"\bmag unit\b", "multi-agency gang unit", regex=True)\
        .str.replace(r"\batf\b", "alcohol tobacco firearms and explosives", regex=True)
    return df


def extract_badge_no(df):
    badges = df.nominee.str.lower().str.strip().str.extract(r"(^\d+) ")
    df.loc[:, "badge_no"] = badges[0]
    return df


def split_award_nominee_name(df):
    df.loc[:, "nominee"] = (
        df.nominee.str.lower()
        .str.strip()
        .str.replace("-", "", regex=False)
        .str.replace("'", "", regex=False)
        .str.replace(
            (
                r"(\(\w+/?\w+\)|special agent |atf agent |agent |detective |deputy |investigative analyst |"
                r"mag unit crime analyst |task force officer |retired detective |supervisory special agent |"
                r"group supervisor |supervisory special agent |s/t |tpr|sgt |mt |lt)"
            ),
            "",
            regex=True,
        )
        .str.replace(r"(\d+)", "", regex=True)
        .str.replace(
            r"(\w+) ?(\w+)? ?(\w+)?, (\w+) ?(\w+)?", r"\4 \5 \1 \2 \3", regex=True
        )
        .str.replace(r"(\w+)  ? ? ?(\w+)", r"\1 \2", regex=True)
        .str.replace("t sean mccaffery", "sean t mccaffery", regex=False)
        .str.replace(r"st (\w+)", r"st\1", regex=True)
        .str.replace(r"mc (\w+)", r"mc\1", regex=True)
        .str.replace("brownrobertson", "brown robertson", regex=False)
        .str.replace("lewiswilliams", "lewis williams", regex=False)
        .str.replace("jonesbrewer", "jones brewer", regex=False)
        .str.replace("martinbrown", "martin brown", regex=False)
        .str.replace("sanclementehaynes", " sanclemente haynes", regex=False)
        .str.replace("oquendojohnson", " oquendo johnson", regex=False)
        .str.replace("trooper", "", regex=False)
    )
    names = df.nominee.str.lower().str.strip().str.extract(r"((\w+) (\w+ )?(.+))")
    df.loc[:, "first_name"] = names[1].str.strip()
    df.loc[:, "last_name"] = names[3].str.strip()
    df.loc[:, "middle_name"] = names[2]
    return df.drop(columns={"nominee"})


def split_award_nominator_name(df):
    df.loc[:, "nominated_by"] = (
        df.nominated_by.str.lower()
        .str.strip()
        .str.replace(" -", "", regex=False)
        .str.replace(r"\.", "", regex=True)
    )
    attributes = df.nominated_by.str.extract(r"(\d+) (\w+), (\w+) ?(\w{1})? (.+)")
    df.loc[:, "nominator_badge_no"] = attributes[0]
    df.loc[:, "nominator_last_name"] = attributes[1]
    df.loc[:, "nominator_first_name"] = attributes[2]
    df.loc[:, "nominator_middle_name"] = attributes[3]
    df.loc[:, "nominator_rank_desc"] = (
        attributes[4]
        .fillna("")
        .str.replace(r"\(|\)", "", regex=True)
        .str.replace(r"^sgt$", "sergeant", regex=True)
        .str.replace(r"^cpt$", "captain", regex=True)
        .str.replace(r"^lt$", "lieutenant", regex=True)
        .str.replace(r"^d$", "detective", regex=True)
        .str.replace(r"^s/ofc$", "senior officer", regex=True)
        .str.replace(r"^ofc$", "officer", regex=True)
        .str.replace(r"^r$", "reserve", regex=True)
        .str.replace(r"^c$", "corporal", regex=True)
    )
    return df.drop(columns={"nominated_by"})


def clean_decision(df):
    df.loc[:, "award_decision"] = (
        df.command_decision.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("approval", "approved", regex=False)
    )
    return df.drop(columns={"command_decision"})


def clean_recommended_award(df):
    df.loc[:, "recommended_award"] = (
        df.recommended_award.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"�s |� |� ", "", regex=True)
        .str.replace("superintendentaward", "superintendent award", regex=False)
    )
    return df


def clean_award(df):
    df.loc[:, "award"] = (
        df.award_disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"�s |� |� ", "", regex=True)
        .str.replace("superintendentaward", "superintendent award", regex=False)
    )
    return df.drop(columns=("award_disposition"))


def drop_rows_where_award_recommended_and_award_is_empty(df):
    return df[~((df.award_decision == "") & (df.award == ""))]


def remove_future_dates(df):
    df.loc[:, "receive_date"] = df.receive_date.str.replace(
        "12/31/9999", "", regex=False
    )
    return df


def clean():
    df = pd.read_csv(deba.data("raw/ipm/new_orleans_pd_commendations_2016_2021.csv"))
    df = (
        df.pipe(clean_column_names)
        .drop(columns={"item_number"})
        .rename(
            columns={
                "date": "recommendation_date",
                "disposition_date": "decision_date",
                "award_date": "receive_date",
            }
        )
        .pipe(remove_future_dates)
        .pipe(extract_rank)
        .pipe(extract_badge_no)
        .pipe(split_award_nominee_name)
        .pipe(split_award_nominator_name)
        .pipe(clean_decision)
        .pipe(clean_ranks, ["rank_desc", "nominator_rank_desc"])
        .pipe(standardize_desc_cols, ["recommended_award", "award_disposition"])
        .pipe(clean_recommended_award)
        .pipe(clean_award)
        .pipe(drop_rows_where_award_recommended_and_award_is_empty)
        .pipe(
            set_values,
            {
                "agency": "New Orleans PD",
            },
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
    )
    return df


if __name__ == "__main__":
    award = clean()

    award.to_csv(deba.data("clean/award_new_orleans_pd_2016_2021.csv"), index=False)

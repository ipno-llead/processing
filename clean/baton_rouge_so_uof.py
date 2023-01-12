import pandas as pd
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def strip_commas(df):
    for col in df.columns:
        df = df.apply(lambda col: col.str.replace("'", "", regex=True))
    return df


def split_names(df):
    names = (
        df.title_of_report.str.lower()
        .str.strip()
        .str.replace(
            r"(use of force:? |\[|\]|(\w+\/\w+\/\w+|-|\w+\-\w+\-\w+|\(|\)|S2888))",
            "",
            regex=True,
        )
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"josephsmith", "joseph smith", regex=False)
        .str.replace(r"damianking", "damian king", regex=False)
        .str.replace(r"^use of force$", "", regex=True)
        .str.replace(r"collins\/", "collins /", regex=True)
        .str.replace(r"^shaniqua$", "shaniqua whitley", regex=True)
        .str.extract(
            r"^(cpl|det|sgt|dy|lt|deputy|detective|\bpl|corporal|sergeant|cp)?\.? ?(\w+)?\.? ?(\w{1})?\.? (\w+)"
        )
    )
    ranks = {
        "sgt": "sergeant",
        "cpl": "corporal",
        "det": "detective",
        "dy": "deputy",
        "lt": "lieutenant",
    }

    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "rank_desc"] = df.rank_desc.map(ranks)
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[3].str.replace(r"date$", "", regex=True)
    return df[~((df.last_name.fillna("") == ""))].drop(columns=["title_of_report"])


def clean_uof_desc(df):
    df.loc[:, "use_of_force_description"] = (
        df.force_used.str.lower()
        .str.strip()
        .str.replace(r"(#|\")", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r";", "; ", regex=False)
        .str.replace(r"warings", "warnings", regex=False)
        .str.replace(r"verba\b", "verbal", regex=True)
        .str.replace(r"\. canister$", "", regex=True)
        .str.replace(r"k9", "k-9", regex=False)
        .str.replace(r"warrnings", "warnings", regex=False)
        .str.replace(
            r"k-9 ?-? surrendere?d to warnings",
            r"surrendered to k-9 warnings",
            regex=True,
        )
        .str.replace(r"surrender\b", "surrendered", regex=True)
        .str.replace(r"apprhension", "apprehension", regex=False)
        .str.replace(r"(other; |;? ?other)", "", regex=True)
        .str.replace(r"apprehension\/no contact", "apprehension no contact", regex=True)
    )
    return df.drop(columns=["force_used"])


def clean_uof_effective(df):
    df.loc[:, "use_of_force_effective"] = (
        df.effectiveness_of_force_used.str.lower()
        .str.strip()
        .str.replace(r"#", "", regex=False)
        .str.replace(r";", "; ", regex=False)
        .str.replace(r"subjec\b", "subject", regex=True)
    )
    return df.drop(columns=["effectiveness_of_force_used"])


def clean_report_date(df):
    dates_and_times = df.date_and_time.str.extract(r"(\w+\/\w+\/\w+) (.+)")
    df.loc[:, "report_date"] = dates_and_times[0]
    df.loc[:, "report_time"] = dates_and_times[1]
    return df.drop(columns=["date_and_time"])


def clean():
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_so/baton_rouge_so_uof_2020.csv"))
        .pipe(clean_column_names)
        .drop(columns=["title_2"])
        .pipe(strip_commas)
        .pipe(split_names)
        .pipe(clean_uof_desc)
        .pipe(clean_uof_effective)
        .pipe(clean_report_date)
        .pipe(set_values, {"agency": "east-baton-rouge-so"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "report_date",
                "uid",
                "use_of_force_description",
                "use_of_force_effective",
            ],
            "uof_uid",
        )
    )

    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/uof_baton_rouge_so_2020.csv"), index=False)

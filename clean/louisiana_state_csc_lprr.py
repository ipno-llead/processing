from lib.clean import clean_dates, clean_names
from lib.columns import clean_column_names
import deba
from lib.uid import gen_uid
import pandas as pd
import re


def standardize_appealed(df):
    df.loc[:, "appealed"] = (
        df.appealed.str.strip()
        .fillna("")
        .str.replace(r"^(\w+) (\w+)", r"\1 - \2", regex=True)
        .str.replace("1st circuit", "to 1st circuit court of appeals", regex=False)
        .str.replace("to supreme court", "and to supreme court of appeals", regex=False)
        .str.replace(
            "LSP - Filed appeal - Yes - to 1st circuit court of appeals",
            "Yes - Louisiana State Police filed appeal to 1st circuit court of appeals",
            regex=False,
        )
    )
    return df


def split_row_with_multiple_appellant(df):
    df = (
        df.drop("appellant", axis=1)
        .join(
            df["appellant"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("appellant"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    df = (
        df.drop("appellant", axis=1)
        .join(
            df["appellant"]
            .str.split("-", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("appellant"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    df.loc[:, "appellant"] = (
        df.appellant.str.replace(r"^Matte$", r"John Matte")
        .str.replace(r"^Perry$", r"Jesse Perry")
        .str.replace(r"^Cook$", r"Louis Cook")
        .str.replace(r"^William$", r"Kenneth Williams")
    )
    return df


def split_appellant_column(df):
    df.loc[:, "appellant"] = (
        df.appellant.str.replace(r", ", " ")
        .str.strip()
        .str.replace(r"D'Berry Jr\. Reuben", "Berry Jr. Reuben O.")
    )

    def split_name(val):
        m = re.match(r"^(.+ Jr\.) (.+)$", val)
        if m is not None:
            return m.group(2), m.group(1)
        m = re.match(r"^(.+ [A-Z]\.) (.+)$", val)
        if m is not None:
            return m.group(1), m.group(2)
        m = re.match(r"^([\w\.]+) (\w+)", val)
        if m is not None:
            return m.group(2), m.group(1)
        m = re.match(r"^\w+$", val)
        if m is not None:
            return "", val
        raise ValueError("unhandled format %s" % val)

    names = pd.DataFrame.from_records(df.appellant.map(split_name))
    df.loc[:, "last_name"] = names.iloc[:, 1]
    names = names.iloc[:, 0].str.split(" ", expand=True)
    df.loc[:, "first_name"] = names.iloc[:, 0]
    df.loc[:, "middle_name"] = names.iloc[:, 1]

    return df.drop(columns=["appellant"])


def assign_additional_appellant_names(df):
    names = pd.read_csv(
        deba.data("raw/louisiana_state_csc/la_lprr_appellants.csv")
    ).rename(columns={"appellant_middle_initial": "appellant_middle_name"})
    for _, row in names.iterrows():
        for col in ["first_name", "last_name", "middle_name"]:
            df.loc[df.docket_no == row.docket_no, col] = row["appellant_%s" % col]
    df.loc[df.docket_no == "93-36-O", "first_name"] = "Edward"
    df.loc[df.docket_no == "93-36-O", "middle_name"] = "A"
    df.loc[df.docket_no == "93-36-O", "last_name"] = "Kuhnest"
    return df


def split_rows_with_multiple_docket_no(df):
    df.loc[:, "docket_no"] = df.docket_no.str.replace(r"- ", "-", regex=False)
    df = (
        df.drop("docket_no", axis=1)
        .join(
            df["docket_no"]
            .str.split("-", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("docket_no"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def correct_docket_no(df):
    def process(row):
        if row.filed_year == "":
            return row.docket_no
        if row.docket_no[:2] != row.filed_year[2:]:
            return row.filed_year[2:] + row.docket_no[2:]
        return row.docket_no

    df.loc[:, "docket_no"] = df.agg(process, axis=1).str.replace(
        r"(-|\/)", "", regex=True
    )
    return df


def clean_appeal_disposition(df):
    df.loc[:, "appeal_disposition"] = (
        df.decision.str.strip()
        .str.lower()
        .str.replace(r"dnied", "denied")
        .str.replace(r"denited", "denied")
        .str.replace(r"ganted", "granted")
        .str.replace(r"settlemenet", "settlement")
        .str.replace(r"(\w)- ", r"\1 - ")
    )
    return df.drop(columns="decision")


def assign_agency(df):
    df.loc[:, "agency"] = "louisiana-state-pd"
    df.loc[:, "data_production_year"] = 2020
    return df


def assign_charging_supervisor(df):
    docket_year = df.tracking_id.str.replace(r"^(\d+)-.+$", r"\1")
    df.loc[:, "charging_supervisor"] = (
        df.charging_supervisor.str.strip()
        .str.replace(r"Flores", "Marlin A. Flores")
        .str.replace(r"Fonten.+", "Paul Fontenot")
    )
    for years, first_letter, full_name in [
        (["95"], "F", "Paul Fontenot"),
        (["96", "97", "98", "99", "00"], "W", "William R. Whittington"),
        (["00", "01", "02", "03"], "L", "Terry C. Landry"),
        (["04", "05", "06", "07"], "W", "Henry Whitehorn"),
        (["07", "08"], "G", "Stanley Griffin"),
        (
            ["08", "09", "10", "11", "12", "13", "14", "15", "16"],
            "E",
            "Michael D. Edmonson",
        ),
        (["12"], "F", "Michael D. Edmonson"),
        (["17", "18", "19", "20"], "R", "Kevin W. Reeves"),
    ]:
        df.loc[
            (docket_year.isin(years)) & (df.charging_supervisor == first_letter),
            "charging_supervisor",
        ] = full_name
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean_rows_w_multiple_dates(df):
    df.loc[:, "filed_date"] = df.filed_date.str.replace(
        r"1/16/09 7/20/09", "", regex=True
    )
    return df


def clean():
    df = pd.read_csv(
        deba.data("raw/louisiana_state_csc/louisianastate_csc_lprr_1991-2020.csv")
    )
    df = clean_column_names(df)
    df = df.rename(
        columns={
            "docket": "docket_no",
            "apellant": "appellant",
            "colonel": "charging_supervisor",
            "filed": "filed_date",
            "rendered": "appeal_disposition_date",
        }
    )
    df = df.drop(columns=["delay"])
    df = (
        df.pipe(standardize_appealed)
        .pipe(split_row_with_multiple_appellant)
        .pipe(split_appellant_column)
        .pipe(split_rows_with_multiple_docket_no)
        .pipe(assign_additional_appellant_names)
        .pipe(clean_rows_w_multiple_dates)
        .pipe(clean_dates, ["filed_date", "appeal_disposition_date"])
        .pipe(correct_docket_no)
        .pipe(clean_appeal_disposition)
        .pipe(assign_agency)
        .rename(columns={"docket_no": "tracking_id"})
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(gen_uid, ["agency", "first_name", "middle_name", "last_name"])
        .pipe(gen_uid, ["agency", "tracking_id", "uid"], "appeal_uid")
        .pipe(assign_charging_supervisor)
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df.drop_duplicates(subset=["tracking_id", "uid"]).reset_index(drop=True)


if __name__ == "__main__":
    df = clean()

    df.to_csv(deba.data("clean/lprr_louisiana_state_csc_1991_2020.csv"), index=False)

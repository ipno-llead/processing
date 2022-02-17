import pandas as pd
import dirk
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def clean_and_split_names(df):
    df.loc[:, "name"] = (
        df.name.str.lower()
        .str.strip()
        .str.replace("bourque,john", "bourque john", regex=False)
        .str.replace("cressionie,justin", "cressionie justin", regex=False)
        .str.replace("deshotel,shannor", "deshotel shannor", regex=False)
        .str.replace("auzennejames", "auzenne james", regex=False)
        .str.replace("st.cyrneil", "st.cyr neil", regex=False)
        .str.replace("anceletjordan", "ancelet jordan", regex=False)
        .str.replace("suarezrichard", "suarez richard")
        .str.replace("martin, justin c", "martin, c justin", regex=False)
        .str.replace(r"\'", "", regex=True)
        .str.replace(
            r"(unknown|records for file|(file)? ?for records? ?(only)?|"
            r"intake booking|corrections intake)|metro narcotics",
            "",
            regex=True,
        )
        .str.replace(r"(\w+)[,\.] ?(\w+)", r"\1, \2", regex=True)
    )
    names = df.name.str.extract(r"(?:(\w+,?|\w+\.?-?\w+,?) )?(?:(\w+) )?(.+)")
    df.loc[:, "last_name"] = names[0].str.replace(",", "", regex=False).fillna("")
    df.loc[:, "middle_name"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2].fillna("")
    return df.drop(columns="name")


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.complaint.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(",", "", regex=False)
        .str.replace("unable to locate", "", regex=False)
        .str.replace(r"with ?drew complaint", "", regex=True)
        .str.replace(
            r"^courtesy/ identification$", "courtesy/identification", regex=True
        )
        .str.replace(r"(\w+) ?/ ?(\w+)", r"\1/\2", regex=True)
        .str.replace("report writing", "departmental reports", regex=False)
        .str.replace("search seizure", "search and seizure", regex=False)
        .str.replace("citizen complaint", "", regex=False)
        .str.replace("biased biased", "bias-based", regex=False)
        .str.replace("fire arm", "firearm", regex=False)
        .str.replace(r"\/force$", "/use of force", regex=True)
        .str.replace(r"^use of unsatisfactory", "unsatisfactory", regex=True)
        .str.replace(r"(file)? ?(for)? ?records? ?(only)?$", "", regex=True)
        .str.replace(r" \bjob\b ", " ", regex=True)
        .str.replace(r"\/performance\/", "/performance of duty/", regex=True)
        .str.replace(r"\/ conduct$", "/conduct unbecoming", regex=True)
    )
    return df.drop(columns="complaint")


def clean_disposition(df):
    df.disposition = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace("non-sustained", "not sustained", regex=False)
        .str.replace(r"^policy fail$", "policy failure", regex=True)
        .str.replace(
            r"(n/a|^file for records ?(only)?|comp\. withdrew|" r"|withdrew com\.)",
            "",
            regex=True,
        )
    )
    return df


def clean_action(df):
    df.loc[:, "action"] = (
        df.leave.str.lower()
        .str.strip()
        .str.replace(r"(l?e?t\.|n/a|shadow sup|con)", "", regex=True)
        .str.replace(r" ?\.? ?rep", "reprimand", regex=True)
        .str.replace("verbalreprimand", "verbal reprimand", regex=False)
        .str.replace(r"^rem\.?(edial)? ?(train)?", "remedial training", regex=True)
        .str.replace(r"^ ?counsel\b", "counseling", regex=True)
        .str.replace("unit use", "unit privileges", regex=False)
    )
    return df.drop(columns="leave")


def drop_rows_undefined_allegations_and_disposition(df):
    return df[~((df.allegation == "") & (df.disposition == ""))]


def clean_complete(df):
    df.loc[:, "investigation_status"] = (
        df.complete.str.lower()
        .str.strip()
        .str.replace("x", "complete", regex=False)
        .str.replace("n/a", "", regex=False)
    )
    return df.drop(columns="complete")


def clean_days(df):
    df.loc[:, "duration_of_action"] = (
        df.days.str.lower()
        .str.strip()
        .str.replace("1", "1 day", regex=False)
        .str.replace("3", "3 days", regex=False)
        .str.replace("inlieu", "in lieu", regex=False)
    )
    return df.drop(columns="days")


def clean_tracking_number_14(df):
    df.loc[:, "tracking_number"] = (
        df.case.str.lower()
        .str.strip()
        .str.replace(r"(\d+)?-?sep-?(\d+)?", r"09-\1", regex=True)
        .str.replace(r"(\d+)?-?oct-?(\d+)?", r"10-\1", regex=True)
        .str.replace(r"(\d+)?-?nov-?(\d+)?", r"11-\1", regex=True)
        .str.replace(r"(\d+)?-?dec-?(\d+)?", r"12-\1", regex=True)
    )
    return df.drop(columns="case")


def clean_level(df):
    df.loc[:, "level"] = (
        df.level.astype(str)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"(\d{1})(\d{1})", r"\1", regex=True)
    )
    return df.drop(columns="level")


def clean_department_desc(df):
    df.loc[:, "department_desc"] = (
        df.emp_assign.str.lower()
        .str.strip()
        .str.replace("off duty", "off-duty", regex=False)
        .str.replace(r"\bcorrect(ion)?\b", "corrections", regex=True)
    )
    return df.drop(columns="emp_assign")


def clean_tracking_number_08(df):
    df.loc[:, "tracking_number"] = (
        df.case.str.lower()
        .str.strip()
        .str.replace(r"(\d+)?-?aug-?(\d+)?", r"12-\1", regex=True)
    )
    return df.drop(columns="case")


def split_rows_with_multiple_allegations(df):
    i = 0
    for idx in df[df.allegation.fillna("").str.contains("/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def clean_action_08(df):
    df.loc[:, "action"] = (
        df.leave.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"lt\.(\w+)", r"letter of \1", regex=True)
        .str.replace(r"ltr?\.?", "letter of", regex=True)
        .str.replace(r"\brep\b", "reprimand", regex=True)
        .str.replace(r"\bcou?ns?\b", "counseling", regex=True)
        .str.replace(r"\bsusp?\b", "suspension", regex=True)
        .str.replace(r"\bterm\b", "termination", regex=True)
        .str.replace("n/a", "", regex=False)
        .str.replace(r"\/", "|", regex=True)
        .str.replace(r"^remid$", "remedial", regex=True)
    )
    return df.drop(columns="leave")


def clean20():
    df = pd.read_csv(
        dirk.data("raw/lafayette_so/lafayette_so_cprr_2015_2020.csv")
    ).pipe(clean_column_names)
    df = (
        df.rename(
            columns={
                "case": "tracking_number",
                "date": "receive_date",
                "emp_assign": "department_desc",
            }
        )
        .pipe(clean_and_split_names)
        .pipe(clean_allegations)
        .pipe(clean_dates, ["receive_date"])
        .pipe(standardize_desc_cols, ["department_desc"])
        .pipe(clean_disposition)
        .pipe(drop_rows_undefined_allegations_and_disposition)
        .pipe(clean_action)
        .pipe(clean_complete)
        .pipe(clean_days)
        .pipe(
            set_values,
            {
                "agency": "Lafayette SO",
            },
        )
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "action", "tracking_number"],
            "allegation_uid",
        )
    )
    return df


def clean14():
    df = (
        pd.read_csv(dirk.data("raw/lafayette_so/lafayette_so_cprr_2009_2014.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "receive_date"})
        .drop(columns="days")
        .pipe(clean_and_split_names)
        .pipe(clean_tracking_number_14)
        .pipe(clean_level)
        .pipe(clean_allegations)
        .pipe(clean_complete)
        .pipe(clean_disposition)
        .pipe(clean_action)
        .pipe(clean_department_desc)
        .pipe(set_values, {"agency": "Lafayette SO"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "action", "tracking_number", "receive_date"],
            "allegation_uid",
        )
    )
    return df


def clean08():
    df = (
        pd.read_csv(dirk.data("raw/lafayette_so/lafayette_so_cprr_2006_2008.csv"))
        .pipe(clean_column_names)
        .drop(columns=["emp"])
        .rename(columns={"date": "receive_date"})
        .pipe(clean_and_split_names)
        .pipe(clean_tracking_number_08)
        .pipe(clean_department_desc)
        .pipe(clean_allegations)
        .pipe(split_rows_with_multiple_allegations)
        .pipe(clean_complete)
        .pipe(clean_level)
        .pipe(clean_action_08)
        .pipe(set_values, {"agency": "Lafayette SO"})
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "action", "tracking_number", "receive_date"],
            "allegation_uid",
        )
    )
    return df


if __name__ == "__main__":
    df20 = clean20()
    df14 = clean14()
    df08 = clean08()
    df20.to_csv(dirk.data("clean/cprr_lafayette_so_2015_2020.csv"), index=False)
    df14.to_csv(dirk.data("clean/cprr_lafayette_so_2009_2014.csv"), index=False)
    df08.to_csv(dirk.data("clean/cprr_lafayette_so_2006_2008.csv"), index=False)

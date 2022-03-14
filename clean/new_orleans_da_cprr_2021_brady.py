from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
import deba
from lib.clean import clean_names, standardize_desc_cols, clean_dates
import pandas as pd


def extract_date_from_pib(df):
    tracking = df.pib_control.str.split("-", 1, expand=True)
    df.loc[:, "receive_date"] = tracking.loc[:, 0].fillna("")
    df.loc[:, "partial_tracking_number"] = tracking.loc[:, 1].fillna("")
    df.loc[:, "tracking_number"] = df.receive_date.str.cat(
        df.partial_tracking_number, sep="-"
    )
    df = df.drop(columns=["pib_control", "partial_tracking_number"])
    return df


def combine_rule_and_paragraph(df):
    df.loc[:, "allegation"] = (
        df.allegation_classification.str.cat(df.allegation, sep=";")
        .str.replace(r"^;", "", regex=True)
        .str.replace(r";$", "", regex=True)
    )
    df = df.drop(columns=["allegation_classification"])
    return df


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"ral[-: ]?condu", r"ral condu", regex=True)
        .str.replace(r"conduc(?:i|t)?", r"conduct", regex=True)
        .str.replace(r"(?:m|1)?o?ral", r"moral", regex=True)
        .str.replace(r"rule[: ]*(\d+)[: ]*", r"rule \1: ", regex=True)
        .str.replace(r"(\d): \d: ?", r"\1: ", regex=True)
        .str.replace(r"rule[ :]+([a-z])", r"rule: \1", regex=True)
        .str.replace("rule: moral conduct", "rule 2: moral conduct", regex=False)
        .str.replace(r" ?; ?", "; ", regex=True)
        .str.replace(r"[ \(]+", " ", regex=True)
        .str.replace(r"p?aragrap[hr]", "paragraph", regex=True)
        .str.replace(r"paragraph [o|0]:? ", "paragraph ", regex=True)
        .str.replace(r"(\d+) - ", r"\1 ", regex=True)
        .str.replace(" infi", " info", regex=False)
        .str.replace(r"adh[ée]rence ?", "adherence ", regex=True)
        .str.replace(
            "paragraph adherence to law", "paragraph 01 adherence to law", regex=False
        )
        .str.replace(
            "paragraph honesty and truthfulness",
            "paragraph 03 honesty and truthfulness",
            regex=False,
        )
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"sust[aä][il]ned", "sustained", regex=True)
    )
    df2 = df.disposition.str.extract(r"((?:not )?sustained[ -\/]?)?(.*)")
    df.loc[:, "action"] = (
        df2.loc[:, 1]
        .str.strip()
        .str.replace(r" ?-", " ", regex=True)
        .str.replace(r"^\.$", "", regex=True)
        .str.replace(r"dismissa$", "dismissal", regex=True)
    )
    df.loc[:, "disposition"] = (
        df2.loc[:, 0].str.strip().str.replace(r"[-\/ ]+$", "", regex=True)
    )
    return df


def clean_allegation_desc(df):
    df.loc[:, "allegation_desc"] = (
        df.directive.str.lower()
        .str.strip()
        .str.replace(r"\r\.\s", "rs", regex=True)
        .str.replace(r"\br\.?s\W*", "", regex=True)
        .str.replace("reltive ", "relative", regex=False)
        .str.replace("licene", "license", regex=False)
        .str.replace("8attery", "battery", regex=False)
        .str.replace("vehivle", "vehicle", regex=False)
        .str.replace("drivers", "driver's", regex=False)
        .str.replace(r"^to (?:wit:?)? *", "to wit: ", regex=True)
        .str.replace(r"(\d)-([a-z])", r"\1 \2", regex=True)
        .str.replace(r"relative(?: ?to)? ", "relative to ", regex=True)
        .str.replace("reckles", "reckless", regex=False)
        .str.replace(r"(\d+)\.(\d+)\.(\d+)", r"\1:\2.\3", regex=True)
        .str.replace(
            r"(\d+:\d+(?:\.\d+)?) (?:relative to )?", r"\1 relative to ", regex=True
        )
        .str.replace(
            r"14:98$",
            "14:98 relative to operating a vehicle while intoxicated ",
            regex=True,
        )
        .str.replace(r"14:35$", "14:35 relative to simple battery", regex=True)
        .str.replace("careleoperation", "careless operation", regex=False)
        .str.replace(r"47-507$", "47-507 relative to display of plate", regex=True)
        .str.replace(r"\Wabue\W", " abuse ", regex=True)
        .str.replace(r"\Wdometic\W", " domestic ", regex=True)
        .str.replace(
            r"14.35.3$", "14:35.3 relative to domestic abuse battery", regex=True
        )
        .str.replace("hit ad run", "14:100 hit and run driving", regex=False)
        .str.replace(
            r"32:863.1$",
            "32:863.1 relative to no proof of liability insurance",
            regex=True,
        )
        .str.replace(
            r"^relative to sexual battery",
            "14:43.1 relative to sexual battery",
            regex=True,
        )
        .str.replace(r"^relative to theft$", "14:67 relative to theft", regex=False)
        .str.replace(
            r"^relative to aggravated incest",
            "14:78.1 relative to aggravated incest",
            regex=True,
        )
        .str.replace(
            r"^relative to attempt and conspiracy to commit offense",
            "14:26 relative to attempt and conspiracy to commit offense",
            regex=True,
        )
        .str.replace(
            r"relative to conspiracy to commit offense",
            "14:26 relative to attempt and conspiracy to commit offense",
            regex=True,
        )
        .str.replace(
            r"^relative to forcible rape",
            "14:42.1 relative to forcible rape",
            regex=True,
        )
        .str.replace(
            r"^relative to no proof of insurance",
            "32:863.1 relative to no proof of liability insurance",
            regex=True,
        )
        .str.replace(
            r"^relative to simple battery$",
            "14:35 relative to simple battery",
            regex=True,
        )
        .str.replace(
            r"^relative to simple assault$",
            "14:38 relative to simple assault",
            regex=True,
        )
        .str.replace(r"^simple assault", "14:38 relative to simple assault", regex=True)
        .str.replace("recklesss", "reckless", regex=False)
        .str.replace(
            r"relative to careless operation of a moveable$",
            "relative to careless operation of a moveable vehicle",
            regex=True,
        )
    )
    return df.drop(columns="directive")


def drop_rows_without_last_name(df):
    df = df[df.last_name != "test"]
    return df.dropna(subset=["last_name"]).reset_index(drop=True)


def clean():
    df = pd.read_csv(deba.data("raw/new_orleans_da/new_orleans_da_cprr_2021_brady.csv"))
    df = clean_column_names(df)
    df = (
        df.rename(
            columns={
                "first name": "first_name",
                "last name": "last_name",
                "finding": "initial_disposition",
            }
        )
        .pipe(extract_date_from_pib)
        .pipe(combine_rule_and_paragraph)
        .pipe(clean_disposition)
        .pipe(clean_allegation_desc)
        .pipe(clean_allegations)
        .pipe(drop_rows_without_last_name)
        .pipe(clean_dates, ["receive_date"])
        .pipe(
            standardize_desc_cols,
            ["initial_disposition", "disposition", "allegation", "allegation_desc"],
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            set_values, {"source_agency": "New Orleans DA", "agency": "New Orleans PD"}
        )
        .pipe(gen_uid, ["agency", "first_name", "last_name"])
        .pipe(
            gen_uid,
            [
                "agency",
                "uid",
                "receive_year",
                "allegation_desc",
                "tracking_number",
                "initial_disposition",
                "disposition",
                "allegation",
            ],
            "allegation_uid",
        )
        .drop_duplicates(subset=["allegation_uid"])
        .pipe(gen_uid, ["uid", "allegation_uid", "source_agency"], "brady_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_new_orleans_da_2021.csv"), index=False)

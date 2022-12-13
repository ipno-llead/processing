import pandas as pd
import deba
from lib.clean import (
    clean_sexes,
    standardize_desc_cols,
    clean_dates,
    clean_races,
    clean_names,
)
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.rows import duplicate_row
import re


def split_rows_with_multiple_officers(df):
    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def split_officer_names(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"^lpcc$", "", regex=True)
        .str.replace(r"lt\.", "lieutenant", regex=True)
        .str.replace(r"(\w+) $", "", regex=True)
        .str.extract(r"(lieutenant)? ?(\w+) ?(\w{1})? (.+)")
    )

    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[3].fillna("")
    return df.drop(columns=["officer"])


def drop_rows_missing_names(df):
    return df[~((df.first_name == "") & (df.last_name == ""))]


def clean_receive_date(df):
    df.loc[:, "receive_date"] = (
        df.date.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"^carry over for 2020$", "", regex=True)
    )
    return df.drop(columns=["date"])


def extract_race_and_sex_columns(df):
    races = df.race_sex_office.str.lower().str.strip().str.extract(r"(^b|^w)")
    df.loc[:, "race"] = (
        races[0]
        .fillna("")
        .str.replace(r"^b$", "black", regex=True)
        .str.replace(r"^w$", "white", regex=True)
    )

    sexes = df.race_sex_office.str.lower().str.strip().str.extract(r"(f$|m$)")

    df.loc[:, "sex"] = (
        sexes[0]
        .fillna("")
        .str.replace(r"^f$", "female", regex=True)
        .str.replace(r"^m$", "male", regex=True)
    )
    return df.drop(columns=["race_sex_office"])


def extract_disposition(df):
    dispositions = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.extract(r"(\bnot sustained\b|sustained|unfounded|\bno violations\b)")
    )

    df.loc[:, "disposition"] = dispositions[0]
    return df


def clean_actions(df):
    df.loc[:, "action"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"\bterminated\b", "termination", regex=True)
        .str.replace(r"\b40 hrs of susp\b", "40-hour suspension", regex=True)
        .str.replace(r" ?\/ ?", " ", regex=True)
        .str.replace(r"^resignation in lieu$", "resignation", regex=True)
        .str.replace(r"transfer 8 hrs", "8-hour transfer", regex=True)
        .str.replace(
            r"^resigned under investigation",
            "resignation while under investigation",
            regex=True,
        )
        .str.replace("refered", "referred", regex=False)
        .str.replace(
            r"(^abandoned$|^handeled at the division$|no violations|^unfounded$|\bnot sustained\b|\bsustained\b)",
            "",
            regex=True,
        )
    )
    return df


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"art\.?", "article", regex=True)
        .str.replace(r"^terminated$", "", regex=True)
        .str.replace(r"takeing\b", "taking", regex=True)
        .str.replace(r"\bherrasment\b", "harrassment", regex=True)
        .str.replace(r"^no disciplin$", "", regex=True)
        .str.replace(r"^po ", "", regex=True)
        .str.replace(r"^(\w{2})\b", r"article \1", regex=True)
        .str.replace(r"\/", ",", regex=True)
        .str.replace(r"(\w{2})\, (\w{2})", r"\1,\2", regex=True)
        .str.replace(r"^fratinization$", "fraternization", regex=True)
    )
    return df.drop(columns=["outcome"])


def strip_leading_aps(df):
    df = df.apply(lambda x: x.str.replace(r"^\'", "", regex=True))
    return df


def extract_action(df):
    actions = (
        df.outcome.str.lower()
        .str.strip()
        .str.extract(
            r"(terminated|termination|resin?gn?ed in lieu of termination"
            r"|resigned prior to invest|resigned prior to completion|written reprimand"
            r"|resigned|resignation|32 hrs suspension|resigned prior to termination)"
        )
    )

    df.loc[:, "action"] = (
        actions[0]
        .fillna("")
        .str.replace(r"resignation", "resigned", regex=False)
        .str.replace(r"invest", "investigation", regex=False)
        .str.replace(r"32 hrs suspension", "32-hour suspension", regex=False)
    )
    return df


def clean_disposition15(df):
    df.loc[:, "disposition"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(r"resign\b", "resigned", regex=True)
        .str.replace(r"(\/|\,)", "; ", regex=True)
        .str.replace(r";? ?terminated", "", regex=True)
        .str.replace(r"\breff?e?r?e?d?\b", "referred", regex=True)
        .str.replace(r"capt\b", "captain", regex=True)
        .str.replace("accidental discharge", "", regex=False)
        .str.replace("crim", "criminal", regex=False)
        .str.replace("earley", "early", regex=False)
        .str.replace(r"^sus-appeal-.+", "sustained", regex=True)
        .str.replace(r"\bda\b", "district attorney's", regex=True)
        .str.replace("tran", "transferred", regex=False)
        .str.replace(
            r" (before)? ?(prior)? ?(to)? ?com(plete)?",
            " before completion",
            regex=True,
        )
        .str.replace(r"completionmander", "completion", regex=False)
        .str.replace(r"resigned; before", "resigned before", regex=False)
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r"\badm\b", "administrative", regex=True)
        .str.replace(r"\((.+)\)", "", regex=True)
        .str.replace(r"adm rem;", "", regex=False)
        .str.replace(r" +$", "", regex=True)
    )

    return df.drop(columns=["outcome"])


def split_rows_with_multiple_officers15(df):
    df.loc[:, "officer"] = df.officer.str.replace(r"\,", r"/ ", regex=True).str.replace(
        r"See #8.+", "", regex=True
    )

    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\/", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def drop_rows_missing_allegations(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower().str.strip().str.replace(r"na", "", regex=True)
    )
    return df[~((df.allegation == ""))]


def split_rows_with_multiple_allegations(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"(&|and)", ",", regex=True)
        .str.replace(r"\, ", ",", regex=True)
    )
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split(",", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"6?-?(lspo|twp\b).+", "", regex=True)
        .str.replace(r"^ +", "", regex=True)
        .str.replace(r" +$", "", regex=True)
        .str.extract(r"(\w+) (\w+) ?(\w+)?")
    )

    df.loc[:, "first_name"] = names[0].fillna("")
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "suffix"] = names[2].fillna("")
    return df[~((df.first_name == "") & (df.last_name == ""))].drop(columns=["officer"])


def allegation_dict():
    df = pd.read_csv(deba.data("raw/lafourche_so/allegation_dict.csv"))
    df.loc[:, "code"] = df.code.str.lower().str.strip().replace(r"^\. ", "", regex=True)
    df.loc[:, "description"] = (
        df.description.str.lower().str.strip().str.replace(r"^art\. ", "", regex=True)
    )

    allegation_dict = dict(zip(df.code, df.description))
    return allegation_dict


def map_allegation_desc(df):
    allegations = allegation_dict()
    df.loc[:, "allegation_desc"] = df.allegation.map(allegations)
    df.loc[:, "allegation_desc"] = df.allegation_desc.str.replace(r"^ ", "", regex=True)
    return df


def correct_actions(df):
    df.loc[
        (df.last_name == "peterson")
        & (df.disposition == "sustained; resigned prior to discipline"),
        "action",
    ] = "resgined prior to discipline"
    df.loc[
        (df.last_name == "durand") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "fields") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "guidry") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "marulli") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "wintzel") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "bearden") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "guilliot") & (df.disposition == "resigned before completion"),
        "action",
    ] = "resigned before completion"
    df.loc[
        (df.last_name == "lymous")
        & (df.disposition == "transferred before completion; appeal; resigned"),
        "action",
    ] = "resigned"
    df.loc[
        (df.last_name == "campo") & (df.disposition == "sustained; resigned"), "action"
    ] = "resigned"
    df.loc[
        (df.last_name == "bogle") & (df.disposition == "sustained; resigned"), "action"
    ] = "resigned"
    df.loc[
        (df.last_name == "guidry") & (df.disposition == "sustained; resigned"), "action"
    ] = "resigned"
    df.loc[
        (df.last_name == "jennings") & (df.disposition == "sustained; resigned"),
        "action",
    ] = "resigned"
    df.loc[
        (df.last_name == "kaufman") & (df.disposition == "sustained; resigned"),
        "action",
    ] = "resigned"
    df.loc[
        (df.last_name == "johnson")
        & (df.disposition == "    resigned; refused to cooperate"),
        "action",
    ] = "resigned"
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def split_rows_with_multiple_officers_10(df):
    df.loc[:, "officer"] = df.officer.str.replace(r"(\,|&)", "/", regex=True)
    i = 0
    for idx in df[df.officer.str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\/", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1
    return df


def split_names_10(df):
    names = (
        df.officer.str.lower()
        .str.strip()
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r"den?tention center ?(staff)?", "", regex=True)
        .str.replace(r"tpd off ricky ross", "ricky ross", regex=False)
        .str.replace(r"cmu officers", "", regex=False)
        .str.replace(
            r"(unknown.+|lps.+|tpd.+|inmate.+|\?|\((\w+)\)|detention.+)", "", regex=True
        )
        .str.replace(r"(\w+) (jr)", r"\1\2", regex=True)
        .str.extract(r"(dty|clerk|lt|[as]gt|capt|co\b)?\.? ?(\w+) ?(\w+)? (\w+)")
    )
    df.loc[:, "rank_desc"] = (
        names[0]
        .str.replace(r"dty\.?", "deputy", regex=True)
        .str.replace(r"lt\.?", "lieutenant", regex=True)
        .str.replace(r"capt\.?", "captain", regex=True)
        .str.replace(r"sgt\.?", "sergeant", regex=True)
    )
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    df.loc[:, "last_name"] = names[3].str.replace(r"(\w+)(jr)", r"\1 \2", regex=True)
    return df[
        ~((df.first_name.fillna("") == "") & (df.last_name.fillna("") == ""))
    ].drop(columns=["officer"])


def clean_race_sex(df):
    officer = (
        df.race_sex_officer.str.lower()
        .str.strip()
        .str.replace(r"(.+)(&|\,)(.+)", "", regex=True)
        .str.replace(r"w/f b/m", "", regex=False)
        .str.replace(r"wb\/m", "", regex=True)
        .str.extract(r"(w|b)\/(m|f)")
    )

    complainant = (
        df.race_sex_complainant.str.lower()
        .str.strip()
        .str.replace(r"(.+)(&|\,|-)(.+)", "", regex=True)
        .str.replace(r"w/f w/m", "", regex=False)
        .str.replace(r"wb\/m", "", regex=True)
        .str.extract(r"(w|b|h)\/(m|f)")
    )

    df.loc[:, "race"] = (
        officer[0]
        .str.replace(r"w", "white", regex=False)
        .str.replace(r"b", "black", regex=False)
    )
    df.loc[:, "sex"] = (
        officer[1]
        .str.replace(r"m", "male", regex=False)
        .str.replace(r"f", "female", regex=False)
    )

    df.loc[:, "complainant_race"] = (
        complainant[0]
        .str.replace(r"w", "white", regex=False)
        .str.replace(r"b", "black", regex=False)
        .str.replace(r"h\b", "hispanic")
    )
    df.loc[:, "complainant_sex"] = (
        complainant[1]
        .str.replace(r"m", "male", regex=False)
        .str.replace(r"f", "female", regex=False)
    )
    return df.drop(columns=["race_sex_officer", "race_sex_complainant"])


def clean_complainant(df):
    df.loc[:, "complainant_type"] = (
        df.complainant.str.lower()
        .str.strip()
        .str.replace(r"(.+)?ia\b(.+)?", "internal affairs", regex=True)
        .str.replace(r"crime stoppers", "public", regex=False)
        .str.replace(r"^(?!(internal affairs|public)).*", "", regex=True)
    )
    return df.drop(columns=["complainant"])


def clean_receive_date10(df):
    df.loc[:, "receive_date"] = (
        df.date.str.replace(r"(\w+)-(\w+)-(\w+)", r"\2/\1/20\3", regex=True)
        .str.lower()
        .str.strip()
        .str.replace(r"jan", "01", regex=False)
        .str.replace(r"oct", r"10", regex=True)
        .str.replace(r"dec", r"12", regex=True)
        .str.replace(r"aug", r"8", regex=True)
        .str.replace(r"jun", r"6", regex=True)
        .str.replace(r"may", r"5", regex=True)
        .str.replace(r"apr", r"4", regex=True)
        .str.replace(r"mar", r"3", regex=True)
        .str.replace(r"feb", r"2", regex=True)
        .str.replace(r"sep", r"9", regex=True)
        .str.replace(r"jul", r"7", regex=True)
        .str.replace(r"nov", r"11", regex=True)
        .str.replace(r"(.+)-(.+)", "", regex=True)
    )
    return df.drop(columns=["date"])


def extract_disposition10(df):
    df.loc[:, "disposition"] = (
        df.outcome.str.lower()
        .str.strip()
        .str.replace(
            r"(terminated|termination|resin?gn?ed in lieu of termination"
            r"|resigned prior to invest|resigned prior to completion|written reprimand"
            r"|resigned|resignation|32 hrs suspension|resigned prior to termination)",
            "",
            regex=True,
        )
        .str.replace(r"w?\/ ?$", "", regex=True)
        .str.replace(r"both.+", "", regex=True)
        .str.replace(r" ?(re issue|\((.+)\)|-)", "", regex=True)
        .str.replace(r"prior to ?", "", regex=True)
        .str.replace(r"(\w+) $", r"\1", regex=True)
        .str.replace(r" on all", "", regex=False)
        .str.replace(r"sustanined", "sustained", regex=True)
        .str.replace(r"ref\b", "referred", regex=True)
        .str.replace(r"sustained eschete only", "sustained", regex=False)
    )
    df.loc[
        (df.last_name == "reed") & (df.tracking_id_og == "13-047"),
        "disposition",
    ] = ""

    return df.drop(columns=["outcome"])


def assign_allegation_desc(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.replace(r"(\w+) (firearm|card)", r"\2", regex=True)
        .str.replace(r"poss of contrab", "loss of contraband", regex=False)
        .str.replace(r"discharg", "discharge", regex=False)
        .str.replace(r"^app?e?n?d?\b", "appendix", regex=True)
        .str.replace(r"susicide", "suicide", regex=False)
        .str.replace(r"^na$", "", regex=True)
        .str.replace(r"^x$", "", regex=True)
    )

    df.loc[
        (df.tracking_id_og == "11-043") & (df.allegation == "hanging"),
        "allegation_desc",
    ] = "hanging"
    df.loc[
        (df.tracking_id_og == "11-020") & (df.allegation == "loss of contraband"),
        "allegation_desc",
    ] = "loss of contraband"
    df.loc[
        (df.tracking_id_og == "11-045") & (df.allegation == "attempted suicide"),
        "allegation_desc",
    ] = "attempted suicide"
    df.loc[
        (df.tracking_id_og == "13-021") & (df.allegation == "insubordination"),
        "allegation_desc",
    ] = "insubordination"
    df.loc[
        (df.tracking_id_og == "10-015") & (df.allegation == "rape"), "allegation_desc"
    ] = "rape"
    df.loc[
        (df.tracking_id_og == "10-016") & (df.allegation == "malfeasance"),
        "allegation_desc",
    ] = "malfeasance"
    df.loc[
        (df.tracking_id_og == "11-004") & (df.allegation == "insubordination"),
        "allegation_desc",
    ] = "insubordination"
    df.loc[
        (df.tracking_id_og == "11-046") & (df.allegation == "domestic"),
        "allegation_desc",
    ] = "domestic"
    df.loc[
        (df.tracking_id_og == "12-012") & (df.allegation == "firearm discharge"),
        "allegation_desc",
    ] = "firearm discharge"
    df.loc[
        (df.tracking_id_og == "12-022") & (df.allegation == "firearm discharge"),
        "allegation_desc",
    ] = "firearm discharge"
    df.loc[
        (df.tracking_id_og == "12-025") & (df.allegation == "card knowledge"),
        "allegation_desc",
    ] = "card knowledge"
    df.loc[
        (df.tracking_id_og == "12-025") & (df.allegation == "card knowledge"),
        "allegation_desc",
    ] = "card knowledge"
    return df


def clean_tracking_id(df):
    df.loc[:, "tracking_id_og"] = df.case.str.replace(
        r"(\w{2})-(\w{3})(.+)", r"\1-\2", regex=True
    ).str.replace(r"na", "", regex=False)
    return df.drop(columns=["case"])


def clean21():
    df = (
        pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2019_2021.csv"))
        .pipe(clean_column_names)
        .rename(columns={"case": "tracking_id"})
        .drop(columns=["complainant", "race_sex"])
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_officer_names)
        .pipe(drop_rows_missing_names)
        .pipe(clean_receive_date)
        .pipe(clean_dates, ["receive_date"])
        .pipe(extract_race_and_sex_columns)
        .pipe(extract_disposition)
        .pipe(clean_actions)
        .pipe(clean_allegation)
        .pipe(standardize_desc_cols, ["action", "tracking_id", "allegation"])
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(set_values, {"agency": "lafourche-so"})
        .pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "tracking_id", "allegation", "disposition", "action"],
            "allegation_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


def clean15():
    df = (
        pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2015_2018.csv"))
        .pipe(clean_column_names)
        .drop(columns=["ia"])
        .rename(columns={"date": "receive_date", "case": "tracking_id"})
        .pipe(strip_leading_aps)
        .pipe(clean_dates, ["receive_date"])
        .pipe(extract_action)
        .pipe(clean_disposition15)
        .pipe(split_rows_with_multiple_officers15)
        .pipe(split_names)
        .pipe(drop_rows_missing_allegations)
        .pipe(split_rows_with_multiple_allegations)
        .pipe(map_allegation_desc)
        .pipe(correct_actions)
        .pipe(set_values, {"agency": "lafourche-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["uid", "allegation", "disposition", "action"], "allegation_uid")
        .drop_duplicates(subset=["allegation_uid"], keep="first")
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


def clean10():
    df = (
        pd.read_csv(deba.data("raw/lafourche_so/lafourche_so_cprr_2010_2014.csv"))
        .pipe(clean_column_names)
        .drop(columns=["ia"])
        .pipe(clean_tracking_id)
        .pipe(standardize_desc_cols, ["tracking_id_og"])
        .pipe(strip_leading_aps)
        .pipe(split_rows_with_multiple_officers_10)
        .pipe(split_names_10)
        .pipe(clean_race_sex)
        .pipe(clean_complainant)
        .pipe(clean_receive_date10)
        .pipe(extract_action)
        .pipe(extract_disposition10)
        .pipe(split_rows_with_multiple_allegations)
        .pipe(map_allegation_desc)
        .pipe(assign_allegation_desc)
        .pipe(set_values, {"agency": "lafourche-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["tracking_id_og", "uid", "allegation", "disposition", "action"],
            "allegation_uid",
        )
        .pipe(gen_uid, ["agency", "tracking_id_og"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    df21 = clean21()
    df15 = clean15()
    df10 = clean10()
    df21.to_csv(deba.data("clean/cprr_lafourche_so_2019_2021.csv"), index=False)
    df15.to_csv(deba.data("clean/cprr_lafourche_so_2015_2018.csv"), index=False)
    df10.to_csv(deba.data("clean/cprr_lafourche_so_2010_2014.csv"), index=False)

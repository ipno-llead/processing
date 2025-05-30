import pandas as pd
import deba
from lib.columns import clean_column_names
from lib.clean import clean_dates, standardize_desc_cols, strip_leading_comma
from lib.uid import gen_uid
from lib.rows import duplicate_row


def split_rows_with_multiple_officers(df):
    df.loc[:, "officer"] = (
        df["officer"]
        .astype(str)
        .str.lower()
        .str.strip()
        .str.strip("'\"")
        .str.replace(r"\s*,\s*|\s*/\s*|\s*<\s*", "/", regex=True)
    )

    i = 0
    for idx in df[df["officer"].str.contains("/")].index:
        s = df.loc[idx + i, "officer"]
        parts = [p.strip() for p in s.split("/") if p.strip()]
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1

    return df


def split_rows_with_multiple_subjects(df):

    df.loc[:, "subject_of_force_name_race_sex"] = (
        df.subject_of_force_name_race_sex.astype(str)
        .str.lower()
        .str.strip()
        .str.strip("'\"")
    )

    df["subject_of_force_name_race_sex"] = df["subject_of_force_name_race_sex"].str.replace(r"\s*//\s*", "//", regex=True)

    i = 0
    for idx in df[df["subject_of_force_name_race_sex"].str.contains("//")].index:
        s = df.loc[idx + i, "subject_of_force_name_race_sex"]
        parts = s.split("//")
        df = duplicate_row(df, idx + i, len(parts))
        for j, part in enumerate(parts):
            df.loc[idx + i + j, "subject_of_force_name_race_sex"] = part.strip()
        i += len(parts) - 1

    return df


def split_name(df):
    df["officer"] = (
        df["officer"]
        .astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"\.", "", regex=True)
    )

    names = df["officer"].str.extract(r"^(\w+)\s+(\w+)$")
    df["first_name"] = names[0]
    df["last_name"] = names[1]

    return df.drop(columns=["officer"])


def split_subject_info(df):
    race_map = {
        "b": "black",
        "w": "white",
    }

    sex_map = {
        "m": "male",
        "f": "female",
    }

    first_names, last_names, races, sexes = [], [], [], []

    for val in df["subject_of_force_name_race_sex"].astype(str):
        val = val.strip().strip('"').strip("'").lower()

        parts = val.split()
        if parts and '/' in parts[-1]:
            race_sex = parts[-1]
            race_sex_parts = race_sex.split('/')
            if len(race_sex_parts) == 2:
                race_code, sex_code = race_sex_parts
                race = race_map.get(race_code, race_code)
                sex = sex_map.get(sex_code, sex_code)
            else:
                race = sex = ""
            name = " ".join(parts[:-1])
        else:
            name = val
            race = sex = ""

        name = name.strip(", ")
        if "," in name:
            name_parts = [n.strip() for n in name.split(",", 1)]
            if len(name_parts) == 2:
                last, first = name_parts
            else:
                first = last = name
        else:
            split_name = name.split()
            if len(split_name) >= 2:
                first, last = split_name[0], " ".join(split_name[1:])
            else:
                first = last = name

        first_names.append(first)
        last_names.append(last)
        races.append(race)
        sexes.append(sex)

    df["subject_of_force_first_name"] = first_names
    df["subject_of_force_last_name"] = last_names
    df["subject_of_force_race"] = races
    df["subject_of_force_sex"] = sexes

    return df.drop(columns=["subject_of_force_name_race_sex"])


def assign_agency(df):
    df.loc[:, "agency"] = "orleans-levee-pd"
    return df


def clean_uof():
    df = (
        pd.read_csv(deba.data("raw/levee_pd/levee_pd_uof_2020_2025.csv"))
        .pipe(clean_column_names)
        .rename(columns={"date": "incident_date", "officer_name": "officer", "force_type_physical_baton_chemical_taser_ois_other": "use_of_force_description", "item_number": "tracking_id_og"})
        .drop(columns=["incident_year", "oldpd_or_ejldpd", "report_signal"])
        .pipe(clean_dates, ["incident_date"])
        .pipe(strip_leading_comma)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_rows_with_multiple_subjects)
        .pipe(split_name)
        .pipe(split_subject_info)
        .pipe(standardize_desc_cols, ["use_of_force_description"])
        .pipe(standardize_desc_cols, ["tracking_id_og"])
        .pipe(assign_agency)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "tracking_id_og",
                "incident_year",
                "incident_month",
                "incident_day",
                "uid", 
            ],
            "uof_uid",
        )
    )
    return df


if __name__ == "__main__":
    uof = clean_uof()
    uof.to_csv(deba.data("clean/uof_levee_pd_2020_2025.csv"), index=False)

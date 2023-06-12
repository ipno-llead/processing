import pandas as pd
from lib.clean import (
    clean_datetimes,
    float_to_int_str,
    standardize_desc_cols,
    clean_races,
    clean_sexes,
    clean_names,
)
import deba
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid


def clean_citizen_gender(df):
    df.loc[:, "citizen_sex"] = (
        df.subjectgender.fillna("")
        .str.lower()
        .str.strip()
        .str.replace("black", "", regex=False)
    )
    return df.drop(columns="subjectgender")


def clean_citizen_eye_color(df):
    df.loc[:, "citizen_eye_color"] = (
        df.subjecteyecolor.fillna("")
        .str.lower()
        .str.strip()
        .str.replace("150", "", regex=False)
    )
    return df.drop(columns="subjecteyecolor")


def clean_citizen_driver_license_state(df):
    df.loc[:, "citizen_driver_license_state"] = (
        df.subjectdriverlicstate.fillna("")
        .str.lower()
        .str.strip()
        .str.replace("black", "", regex=False)
    )
    return df.drop(columns="subjectdriverlicstate")


def extract_assigned_district(df):
    districts = (
        df.officerassignment.str.lower().str.strip().str.extract(r"((.+) district)")
    )

    df.loc[:, ["assigned_district"]] = districts[0].str.replace(r"  +", " ", regex=True)
    return df


def extract_stop_results_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(stop results\: (\w+) ?(\w+)?))")
    )

    df.loc[:, "stop_results"] = (
        col[0]
        .str.replace(r"(stop results\: )", "", regex=True)
        .str.replace(r"^l$", "", regex=True)
    )
    return df


def extract_subject_type_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(subject type\: (\w+)))")
    )

    df.loc[:, "subject_type"] = col[0].str.replace(r"subject type\: ", "", regex=True)
    return df


def extract_search_occurred_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(search occurred\: (\w+)))")
    )

    df.loc[:, "search_occurred"] = col[0].str.replace(
        r"search occurred\: ", "", regex=True
    )
    return df


def extract_search_types_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(search types\: (\w+)?\-? ?(\w+)?\(?(\w{1})?\)?))")
    )

    df.loc[:, "search_types"] = col[0].str.replace(r"search types\: ", "", regex=True)
    return df


def extract_evidence_siezed_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(evidence seized\: (\w+)))")
    )

    df.loc[:, "evidence_seized"] = col[0].str.replace(
        r"evidence seized\: ", "", regex=True
    )
    return df


def extract_evidence_types_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(evidence types\: (\w+) ?(\w+)?\(?(\w{1})?\)?))")
    )

    df.loc[:, "evidence_types"] = col[0].str.replace(
        r"evidence types\: ", "", regex=True
    )
    return df


def extract_legal_basis_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(legal basises\: (\w+) ?(\w+)? ?(\w+)?))")
    )

    df.loc[:, "legal_basis"] = col[0].str.replace(r"legal basises\: ", "", regex=True)
    return df


def extract_consent_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(consent to search\: (\w+)))")
    )

    df.loc[:, "consent_to_search"] = col[0].str.replace(
        r"consent to search\: ", "", regex=True
    )
    return df


def extract_consent_form_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(consent form completed\: (\w+)))")
    )

    df.loc[:, "consent_form_completed"] = col[0].str.replace(
        r"consent form completed\: ", "", regex=True
    )
    return df


def extract_body_search_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(stripbody cavity search\: (\w+)))")
    )

    df.loc[:, "strip_body_cavity_search"] = col[0].str.replace(
        r"stripbody cavity search\: ", "", regex=True
    )
    return df


def extract_exit_vehicle_column(df):
    col = (
        df.actionstaken.str.lower()
        .str.strip()
        .str.extract(r"(?:(exit vehicle\: (\w+)))")
    )

    df.loc[:, "exit_vehicle"] = col[0].str.replace(r"exit vehicle\: ", "", regex=True)
    return df.drop(columns="actionstaken")


def clean_assigned_department(df):
    df.loc[:, "assigned_department"] = (
        df.officerassignment.str.lower()
        .str.strip()
        .str.replace("pib", "public integrity bureau", regex=False)
        .str.replace("fob", "field operations bureau", regex=False)
        .str.replace("msb", "management services bureau", regex=False)
        .str.replace("superintendent", "office of the superintendent", regex=False)
        .str.replace("isb", "investigations and support bureau", regex=False)
        .str.replace(r"((.+) district)", "", regex=True)
    )
    return df.drop(columns="officerassignment")


def consolidate_name_and_badge_columns(df):
    df.loc[:, "officer1badgenumber"] = (
        df.officer1badgenumber.fillna("")
        .astype(str)
        .str.replace(r"^(\w+)$", r"(\1)", regex=True)
    )
    df.loc[:, "officer2badgenumber"] = (
        df.officer2badgenumber.fillna("")
        .astype(str)
        .str.replace(r"^(\w+)\.(\w+)$", r"(\1)", regex=True)
    )

    df.loc[:, "officer1name"] = df.officer1name.str.cat(df.officer1badgenumber, sep=" ")
    df.loc[:, "officer2name"] = df.officer2name.fillna("").str.cat(
        df.officer2badgenumber, sep=" "
    )

    df.loc[:, "officer_names_and_badges"] = df.officer1name.str.cat(
        df.officer2name, sep="/"
    )
    df.loc[:, "officer_names_and_badges"] = (
        df.officer_names_and_badges.str.lower()
        .str.strip()
        .str.replace(r"\/$", "", regex=True)
        .str.replace(r"(\w+) \/ ?(.+)", r"\1/\2", regex=True)
    )
    return df.drop(
        columns=[
            "officer1badgenumber",
            "officer2badgenumber",
            "officer1name",
            "officer2name",
        ]
    )


def split_rows_with_multiple_officers(df):
    df = (
        df.drop("officer_names_and_badges", axis=1)
        .join(
            df["officer_names_and_badges"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("officer_names_and_badges"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def split_names_and_extract_rank_badge(df):
    df.loc[:, "officer_names_and_badges"] = (
        df.officer_names_and_badges.str.replace(r"^p\.o\.?(\w+)", r"p.o \1", regex=True)
        .str.replace(r"^(\w+)\, (\w+) \((\w+)\)$", r"\2 \1 (\3)", regex=True)
        .str.replace(r"(\w+)\,(\w+)", r"\2 \1", regex=True)
        .str.replace(r"\.", "", regex=True)
        .str.replace(r"^\((\w+)\)$", "", regex=True)
        .str.replace(r"  +", " ", regex=True)
        .str.replace(r"cmdr\.?", "commander", regex=True)
        .str.replace(r"sgt\.?", "sergeant", regex=True)
        .str.replace(r"det\.?", "detective", regex=True)
        .str.replace(r"capt\.?", "captain", regex=True)
        .str.replace(r"^po ?(i|iii|iv|1|2|3)? ", "officer ", regex=True)
        .str.replace(r"lt\.?", "lieutenant", regex=True)
        .str.replace(r"^tperez \((\w+)\)$", r"t perez (\1)", regex=True)
        .str.replace(r"^mgennaro \((\w+)\)$", r"m gennaro (\1)", regex=True)
        .str.replace(r"^kdoucette \((\w+)\)$", r"k doucette (\1)", regex=True)
        .str.replace(r"^tmurray \((\w+)\)$", r"t murray (\1)", regex=True)
        .str.replace(r"(\w+)\,? (\w{2}) \((\w+)\)$", r"\1 (\3) \2", regex=True)
        .str.replace(r"\]", "", regex=True)
        .str.replace("reginaldcook", "reginald cook", regex=False)
        .str.replace("simmons751", "simmons", regex=False)
        .str.replace(r"^ghill", r"g hill", regex=True)
        .str.replace(r"kdunnaway \((\w+)\)$", r"k dunnaway (\1)", regex=True)
        .str.replace(r"jwalker \((\w+)\)$", r"j walker (\1)", regex=True)
    )

    names = df.officer_names_and_badges.str.extract(
        r"^(?:(commander|officer|sergeant|captain|detective|lieutenant))? "
        r"?(?:(\w+) )?(?:(\w+) )?(?:(\w+\-?\w+?) )\((\w+)\) ?(.+)?$"
    )
    df.loc[:, "rank_desc"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    df.loc[:, "middle_name"] = names[2].fillna("")
    df.loc[:, "last_name"] = names[3].fillna("")
    df.loc[:, "suffix"] = names[5].fillna("")
    df.loc[:, "badge_number"] = names[4].str.replace(r"\(|\)", "", regex=True)
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df.drop(columns=["suffix", "officer_names_and_badges"])


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def split_names22(df):
    names = df.officer_name.str.lower().str.strip().str.extract(r"^(\w+-?\w+?) ?(\w{1})?\.? (.+)")

    df.loc[:, 'first_name'] = names[0]
    df.loc[:, "middle_name"] = names[1]
    df.loc[:, "last_name"] = names[2]
    return df.drop(columns=["officer_name"])


def extract_action_cols(df):
    stop_results = df.actionstaken.str.lower().str.strip().str.extract(r"(stop results: \w+ ?\w+? \w+?;?s?t?o?p? ?r?e?s?u?l?t?s?:? ?\w+? ?\w+? ?\w+?);(subject)")
    sub_type = df.actionstaken.str.lower().str.strip().str.extract(r"(subject type: \w+ ?\w+? ?\w+?)")
    search_occ = df.actionstaken.str.lower().str.strip().str.extract(r"(search occurred: \w+)")
    evidence_seized = df.actionstaken.str.lower().str.strip().str.extract(r"(evidence seized: \w+)")
    legal_basis = df.actionstaken.str.lower().str.strip().str.extract(r"(legal basises: \w+ ?\w+ ?\w+?);(consent)")
    consent = df.actionstaken.str.lower().str.strip().str.extract(r"(consent form completed: \w+)")
    strip = df.actionstaken.str.lower().str.strip().str.extract(r"(stripbody cavity search: \w+ ?\w+?)")

    df.loc[:, "stop_results"] = stop_results[0].str.replace(r"^(stop)(.+);.+", r"\2", regex=True).str.replace(r"^ ?results.+", "", regex=True)\
                                               .str.replace(r"stop results: ", "", regex=False)
    
    df.loc[:, "citizen_type"] = sub_type[0].str.replace(r"subject type: ", "", regex=False)
    df.loc[:, "search_occurred"] = search_occ[0].str.replace(r"search occurred: ", "", regex=False)
    df.loc[:, "evidence_seized"] = evidence_seized[0].str.replace(r"evidence seized: ", "", regex=False)
    df.loc[:, "legal_basis"] = legal_basis[0].str.replace(r"legal basises: ", "", regex=False)
    df.loc[::, "consent_to_search"] = consent[0].str.replace(r"n$", "no", regex=True).str.replace(r"consent form completed: ", "", regex=False)
    df.loc[:, "strip_body_cavity_search"] = strip[0].str.replace(r"stripbody cavity search: ", "", regex=False)
    return df.drop(columns=["actionstaken"]) 


def clean():
    df = (
        pd.read_csv(deba.data("raw/ipm/new_orleans_pd_stop_and_search_2007_2021.csv"))
        .pipe(clean_column_names)
        .drop(columns=["subjectage"])
        .rename(
            columns={
                "subjectheight": "citizen_height",
                "subjectweight": "citizen_weight",
                "subjecthaircolor": "citizen_hair_color",
                "subjectid": "citizen_id",
                "subjectrace": "citizen_race",
                "carnumber": "vehicle_number",
                "vehicleyear": "vehicle_year",
                "vehiclemodel": "vehicle_model",
                "vehiclestyle": "vehicle_style",
                "vehiclecolor": "vehicle_color",
                "vehiclemake": "vehicle_make",
                "eventdate": "stop_and_search_datetime",
                "zip": "zip_code",
                "blockaddress": "stop_and_search_location",
                "stop_and_search__field_interviews__fieldinterviewid": "tracking_id",
                "fic_officersnames_10_21_2021_fieldinterviewid": "stop_and_search_interview_id_2",
                "nopd_item": "item_number",
                "stopdescription": "stop_reason",
            }
        )
        .pipe(clean_races, ["citizen_race"])
        .pipe(clean_citizen_gender)
        .pipe(clean_citizen_eye_color)
        .pipe(clean_citizen_driver_license_state)
        .pipe(extract_assigned_district)
        .pipe(clean_assigned_department)
        .pipe(extract_stop_results_column)
        .pipe(extract_subject_type_column)
        .pipe(extract_search_occurred_column)
        .pipe(extract_evidence_siezed_column)
        .pipe(extract_evidence_types_column)
        .pipe(extract_body_search_column)
        .pipe(extract_legal_basis_column)
        .pipe(extract_consent_column)
        .pipe(extract_consent_form_column)
        .pipe(extract_search_types_column)
        .pipe(extract_exit_vehicle_column)
        .pipe(consolidate_name_and_badge_columns)
        .pipe(split_rows_with_multiple_officers)
        .pipe(split_names_and_extract_rank_badge)
        .pipe(clean_datetimes, ["stop_and_search_datetime"])
        .pipe(
            standardize_desc_cols,
            [
                "vehicle_number",
                "vehicle_model",
                "vehicle_make",
                "zone",
                "stop_and_search_location",
                "stop_reason",
                "vehicle_style",
                "vehicle_color",
                "citizen_hair_color",
                "item_number",
            ],
        )
        .pipe(
            float_to_int_str,
            [
                "citizen_height",
                "citizen_weight",
                "citizen_hair_color",
                "vehicle_year",
                "zip_code",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "citizen_id",
                "stop_reason",
                "evidence_seized",
            ],
            "stop_and_search_uid",
        )
        .drop_duplicates(subset="stop_and_search_uid")
    )
    citizen_df = df[
        [
            "citizen_id",
            "citizen_race",
            "citizen_height",
            "citizen_weight",
            "citizen_hair_color",
            "citizen_driver_license_state",
            "citizen_sex",
            "citizen_eye_color",
            "stop_and_search_uid",
            "agency",
        ]
    ]
    citizen_df = citizen_df.pipe(
        gen_uid,
        [
            "citizen_id",
            "citizen_race",
            "citizen_height",
            "citizen_weight",
            "citizen_hair_color",
            "citizen_driver_license_state",
            "citizen_sex",
            "citizen_eye_color",
            "stop_and_search_uid",
            "agency",
        ],
        "citizen_uid",
    )

    df = df.drop(
        columns=[
            "citizen_id",
            "citizen_race",
            "citizen_height",
            "citizen_weight",
            "citizen_hair_color",
            "citizen_driver_license_state",
            "citizen_sex",
            "citizen_eye_color",
        ]
    )
    return df, citizen_df


def concat_dfs(df):
    df = df.rename(columns={"23_2141_officersnames_id": "officernames_id"})
    dfa = df.copy()
    dfa = dfa.drop(columns=["officer2name", "officer2badgenumber"]).rename(columns={"officer1name": "officer_name", "officer1badgenumber": "officer_badgenumber"})

    dfb = df.copy()
    dfb = dfb.drop(columns=["officer1name", "officer1badgenumber"]).rename(columns={"officer2name": "officer_name", "officer2badgenumber": "officer_badgenumber"})
    
    df = pd.concat([dfa, dfb])
    return df


def clean23():
    df = (pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_sas_2021_2023.csv"))
          .pipe(clean_column_names)
          .pipe(concat_dfs)
          .drop(columns=["blockadd", "subjectage", "subjectheight", "subjectweight", "lastmodifieddatetime", "officernames_id"])
          .rename(columns={"23_2141_officersnames_fieldinterviewid": "stop_and_search_interview_id_2", 
                           "stop_and_search__field_interviews_2023_03_06_id": "stop_and_search_interview_id",
                           "stop_and_search__field_interviews_2023_03_06_fieldinterviewid": "tracking_id_og",
                           "nopd_item": "item_number", "officerassignment": "assigned_district",
                           "stopdescription": "stop_reason",
                           "vehicleyear": "vehicle_year",
                           "vehiclemake": "vehicle_make",
                           "vehiclemodel": "vehicle_model",
                           "vehiclestyle": "vehicle_style",
                           "vehiclecolor": "vehicle_color",
                           "carnumber": "vehicle_number",
                           "subjectid": "citizen_id",
                           "subjectrace": "citizen_race", 
                           "subjectgender": "citizen_sex",
                           "subjecthasphotoid": "citizen_has_photo_id",
                           "subjecteyecolor": "citizen_eye_color",
                           "subjecthaircolor": "citizen_hair_color",
                           "subjectdriverlicstate": "citizen_driver_license_state",
                           "zip": "zip_code", "officer_badgenumber": "badge_number"})
          .pipe(float_to_int_str, ["vehicle_year", "zip_code",  "citizen_id", "badge_number"])
          .pipe(clean_races, ["citizen_race"])
          .pipe(clean_sexes, ["citizen_sex"])
          .pipe(split_names22)
          .pipe(clean_names, ["first_name", "middle_name", "last_name"])
          .pipe(extract_action_cols)
          .pipe(standardize_desc_cols, ["item_number", "assigned_district", "stop_reason", "vehicle_year", "vehicle_make", "vehicle_model", "vehicle_style", "vehicle_color",
                                        "vehicle_number", "citizen_id",  
                                        "citizen_has_photo_id", "citizen_eye_color", "citizen_hair_color", 
                                        "citizen_driver_license_state", "zip_code"])
          .pipe(set_values, {"agency": "new-orleans-pd"})
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["agency", "tracking_id_og"], "tracking_id")
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "citizen_id",
                "stop_reason",
            ],
            "stop_and_search_uid",
        )
    )
    citizen_df = df[
        [
            "citizen_id",
            "citizen_race",
            "citizen_hair_color",
            "citizen_driver_license_state",
            "citizen_sex",
            "citizen_has_photo_id",
            "citizen_eye_color",
            "stop_and_search_uid",
            "citizen_type",
            "agency",
        ]
    ]
    citizen_df = citizen_df.pipe(
        gen_uid,
        [
            "citizen_id",
            "citizen_race",
            "citizen_hair_color",
            "citizen_driver_license_state",
            "citizen_sex",
            "citizen_has_photo_id",
            "citizen_eye_color",
            "stop_and_search_uid",
            "citizen_type",
            "agency",
        ],
        "citizen_uid",
    )

    df = df.drop(
        columns=[
            "citizen_id",
            "citizen_race",
            "citizen_hair_color",
            "citizen_driver_license_state",
            "citizen_sex",
            "citizen_has_photo_id",
            "citizen_type",
            "citizen_eye_color",
        ]
    )
    return df, citizen_df


def concat_output(off_dfa, off_dfb, cit_dfa, cit_dfb):
    df = pd.concat([off_dfa, off_dfb],  axis=0)
    citizen_df = pd.concat([cit_dfa, cit_dfb], axis=0)
    return df, citizen_df


if __name__ == "__main__":
    df10, citizen_df10 = clean()
    df23, citizen_df23, = clean23()
    df, citizen_df = concat_output(df10, df23, citizen_df10, citizen_df23)
    df.to_csv(deba.data("clean/sas_new_orleans_pd_2010_2023.csv"), index=False)
    citizen_df.to_csv(
        deba.data("clean/sas_cit_new_orleans_pd_2010_2023.csv"), index=False
    )

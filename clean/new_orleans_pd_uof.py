import deba
import pandas as pd

from lib.clean import clean_sexes, standardize_desc_cols
from lib.columns import set_values, clean_column_names
from lib.clean import clean_races, clean_names, clean_dates, clean_sexes
from lib.uid import gen_uid


def split_officer_rows(df):
    officer_columns = [
        "officer_name",
        "race",
        "sex",
        "age",
        "years_of_service",
        "use_of_force_description",
        "use_of_force_level",
        "use_of_force_effective",
        "officer_injured",
    ]
    for col in officer_columns:
        df.loc[:, col] = df[col].str.split(r" \| ")

    def create_officer_lists(row: pd.Series):
        d = row.loc[officer_columns].loc[row.notna()].to_dict()
        min_len = min(len(v) for v in d.values())
        if min_len < max(len(v) for v in d.values()):
            print(
                "WARNING: inconsistent sub-values count detected\n%s" % d,
                file=sys.stderr,
            )
        try:
            return [[row.uof_uid] + [d[k][i] for k in d.keys()] for i in range(min_len)]
        except IndexError as e:
            e.args = ("%s\n\nrow: %s\n\nd: %s" % (e.args[0], row, d),)
            raise

    officer_series = df.apply(create_officer_lists, axis=1)
    df = pd.DataFrame(
        [element for list_ in officer_series for element in list_],
        columns=["uof_uid"] + officer_columns,
    )

    return df


def split_officer_names(df):
    names = df.officer_name.str.extract(r"(\w+)\, ?(\w+)?")
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    return df.drop(columns=["officer_name"])[~((df.last_name.fillna("") == ""))]


def split_citizen_rows(df):
    citizen_columns = [
        "citizen_race",
        "citizen_sex",
        "citizen_hospitalized",
        "citizen_injured",
        "citizen_influencing_factors",
        "citizen_distance_from_officer",
        "citizen_age",
        "citizen_build",
        "citizen_height",
        "citizen_arrested",
        "citizen_arrest_charges",
    ]
    for col in citizen_columns:
        df.loc[:, col] = df[col].str.split(r" \| ")

    def create_citizen_lists(row: pd.Series):
        d = row.loc[citizen_columns].loc[row.notna()].to_dict()
        return [
            [row.uof_uid] + [d[k][i] for k in d.keys()]
            for i in range(max(len(v) for v in d.values()))
        ]

    citizen_series = df.apply(create_citizen_lists, axis=1)
    df = pd.DataFrame(
        [element for list_ in citizen_series for element in list_],
        columns=["uof_uid"] + citizen_columns,
    )

    return df


def clean_tracking_id(df):
    df.loc[:, "tracking_id"] = (
        df.tracking_id.str.lower().str.strip().str.replace(r"^20", "ftn20", regex=True)
    )
    return df


def clean_originating_bureau(df):
    df.loc[:, "originating_bureau"] = (
        df.originating_bureau.str.lower()
        .str.strip()
        .str.replace(r"^(\w+) - ", "", regex=True)
    )
    return df


def clean_division_level(df):
    df.loc[:, "division_level"] = (
        df.division_level.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("police ", "", regex=False)
        .str.replace(" division", "", regex=False)
        .str.replace(r"\bisb\b", "investigative services", regex=True)
        .str.replace(r" ?staff", "", regex=True)
        .str.replace(" section", "", regex=False)
        .str.replace(" unit", "", regex=False)
        .str.replace(" team", "", regex=False)
        .str.replace("not nopd", "", regex=False)
        .str.replace("pib", "", regex=False)
    )
    return df


def clean_division(df):
    df.loc[:, "division"] = (
        df.division.str.lower()
        .str.strip()
        .str.replace(" section", "", regex=False)
        .str.replace(" team", "", regex=False)
        .str.replace(r"\bisb\b", "investigative services", regex=True)
        .str.replace(r" ?staff", "", regex=True)
        .str.replace(r"^b$", "", regex=True)
        .str.replace(r"\btact\b", "tactical", regex=True)
        .str.replace(r" \| ", "; ", regex=True)
        .str.replace(r"^admin$", "administration", regex=True)
        .str.replace(r" persons$", "", regex=True)
        .str.replace(r"d\.?i\.?u\.?", "", regex=True)
        .str.replace(r"investigation$", "investigations", regex=True)
        .str.replace("unknown", "", regex=False)
        .str.replace(r"^cid$", "criminal investigations", regex=True)
    )
    return df


def clean_unit(df):
    df.loc[:, "unit"] = (
        df.unit.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(" unit", "", regex=False)
        .str.replace("unknown", "", regex=False)
        .str.replace(r"null \| ", "", regex=True)
        .str.replace(r"^patr?$", "patrol", regex=True)
        .str.replace(r"^k$", "k-9", regex=True)
        .str.replace(r" other$", "", regex=True)
        .str.replace("dwi", "district investigative unit", regex=False)
        .str.replace(r"^uof$", "use of force", regex=True)
        .str.replace(r"^admin$", "administration", regex=True)
        .str.replace(r"\btact\b", "tactical", regex=True)
        .str.replace(r" persons diu$", "", regex=True)
        .str.replace(r"d\.?i\.?u\.?", "", regex=True)
        .str.replace(" section", "", regex=False)
        .str.replace(" staff", "", regex=False)
        .str.replace(r"v\.o\.\w\.s\.", "violent offender warrant squad", regex=True)
    )
    return df


def clean_working_status(df):
    df.loc[:, "working_status"] = (
        df.working_status.str.lower()
        .str.strip()
        .str.replace("unknown working status", "", regex=False)
        .str.replace("rui", "resigned under investigation", regex=False)
        .str.replace("off duty", "off-duty", regex=False)
        .str.replace("workingre", "working", regex=False)
    )
    return df


def clean_shift(df):
    df.loc[:, "shift_time"] = (
        df.shift_time.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("unknown shift hours", "", regex=False)
        .str.replace("8a-4p", "between 8am-4pm", regex=False)
    )
    return df


def clean_disposition(df):
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"uof", "use of force", regex=False)
        .str.replace(r"member\'s (.+)", "actions were in violation of new orleans pd policy", regex=True)
    )
    return df


def clean_service_type(df):
    df.loc[:, "service_type"] = (
        df.service_type.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" \| ", "; ", regex=True)
        .str.replace("not used", "", regex=False)
    )
    return df


def clean_light_condition(df):
    df.loc[:, "light_condition"] = (
        df.light_condition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"null \| ", "", regex=True)
    )
    return df


def clean_weather_condition(df):
    df.loc[:, "weather_condition"] = (
        df.weather_condition.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r"null \| ", "", regex=True)
    )
    return df


def clean_use_of_force_reason(df):
    df.loc[:, "use_of_force_reason"] = (
        df.use_of_force_reason.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("refuse", "refused", regex=False)
        .str.replace("resisting", "resisted", regex=False)
        .str.replace(r"^no$", "", regex=True)
        .str.replace(r"w\/", "with ", regex=True)
        .str.replace(" police ", " ", regex=False)
    )
    return df


def clean_use_of_force_description(df):
    df.loc[:, "use_of_force_description"] = (
        df.use_of_force_description.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("nontrad", "non traditional", regex=False)
        .str.replace(r"\bcew\b", "conducted electrical weapon", regex=True)
        .str.replace(r"(\w+)\((\w+)\)", r"\1 (\2)", regex=True)
        .str.replace(r"\(\)$", ")", regex=True)
        .str.replace(r"^- ", "", regex=True)
        .str.replace(r"^#name\?$", "", regex=True)
        .str.replace(r"\bvehpursuits\b", "vehicle pursuits", regex=True)
        .str.replace(r"w\/(\w+)", r"with \1", regex=True)
        .str.replace(r"\bwep\b", "weapon", regex=True)
        .str.replace(r"\bnonstrk\b", "non-strike", regex=True)
        .str.replace(r"\/pr-24 ", " ", regex=True)
    )
    return df


def clean_citizen_arrest_charges(df):
    df.loc[:, "citizen_arrest_charges"] = (
        df.citizen_arrest_charges.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("flight,", "flight;", regex=False)
        .str.replace("null", "", regex=False)
        .str.replace("no", "", regex=False)
    )
    return df


def clean_citizen_age(df):
    df.loc[:, 'citizen_age'] = df.citizen_age.str.extract('(\d+)', expand=False)
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def extract_use_of_force_level(df):
    levels = df.use_of_force_description.str.extract(r"^(l[1234])")
    df.loc[:, "use_of_force_level"] = levels[0]

    df.loc[:, "use_of_force_description"] = (df.use_of_force_description.str.replace(r"^(l[1234]) ?-? ?", "", regex=True)
    )
    return df 


def clean_citizen_cols(df):
    injured = df.citizen_injured.str.lower().str.strip().str.extract(r"(no|yes)")
    df.loc[:, "citizen_injured"] = injured[0]

    hospitalized = df.citizen_hospitalized.str.lower().str.strip().str.extract(r"(no|yes)")
    df.loc[:, "citizen_hospitalized"] = hospitalized[0]

    df.loc[:, "citizen_influencing_factors"] = (df.citizen_influencing_factors.str.lower().str.strip()
                                                  .str.replace(r"^((\w{1,4})|(.+ feet .+))$", "", regex=True)
                                                )

    distance = df.citizen_distance_from_officer.str.lower().str.strip().str.extract(r"(.+ feet .+)")
    df.loc[:, "citizen_distance_from_officer"] = distance[0]

    build = df.citizen_build.str.lower().str.strip().str.extract(r"(small|medium|large|xlarge)")

    df.loc[:, "citizen_build"] = build[0]

    df.loc[:, "citizen_height"] = df.citizen_height.str.lower().str.strip().str.replace(r"(yes|no)", "", regex=True)
    return df


def clean_uof():
    dfa = (
        pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_uof_2016_2021.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "filenum": "tracking_id",
                "occurred_date": "uof_occur_date",
                "shift": "shift_time",
                "use_of_force_type": "use_of_force_description",
                "subject_ethnicity": "citizen_race",
                "subject_gender": "citizen_sex",
                "subject_hospitalized": "citizen_hospitalized",
                "subject_injured": "citizen_injured",
                "subject_distance_from_officer": "citizen_distance_from_officer",
                "subject_arrested": "citizen_arrested",
                "subject_arrest_charges": "citizen_arrest_charges",
                "subject_influencing_factors": "citizen_influencing_factors",
                "subject_build": "citizen_build",
                "subject_age": "citizen_age",
                "subject_height": "citizen_height",
                "officer_race_ethnicity": "race",
                "officer_gender": "sex",
                "officer_age": "age",
                "officer_years_of_service": "years_of_service",
                "tracking_id": "tracking_id_og",
            }
        )
        .pipe(clean_weather_condition)
        .pipe(clean_light_condition)
        .pipe(clean_disposition)
        .pipe(clean_shift)
        .pipe(clean_originating_bureau)
        .pipe(clean_use_of_force_reason)
        .pipe(clean_division)
        .pipe(clean_division_level)
        .pipe(clean_unit)
        .pipe(clean_tracking_id)
        .pipe(clean_service_type)
        .pipe(clean_working_status)
        .pipe(clean_dates, ["uof_occur_date"])
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(
            gen_uid,
            [
                "weather_condition",
                "light_condition",
                "disposition",
                "uof_occur_year",
                "uof_occur_month",
                "uof_occur_day",
                "use_of_force_reason",
                "division",
                "division_level",
                "unit",
                "tracking_id",
            ],
            "uof_uid",
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
    )
    dfb = dfa[
        [
            "officer_name",
            "race",
            "sex",
            "age",
            "years_of_service",
            "use_of_force_description",
            "use_of_force_level",
            "use_of_force_effective",
            "officer_injured",
            "uof_uid",
        ]
    ]
    dfb = (
        dfb.pipe(split_officer_rows)
        .pipe(split_officer_names)
        .pipe(clean_use_of_force_description)
        .pipe(clean_races, ["race"])
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_names, ["last_name", "first_name"])
        .pipe(
            standardize_desc_cols,
            [
                "use_of_force_description",
                "use_of_force_level",
                "use_of_force_effective",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .drop_duplicates(subset=["uid", "uof_uid"])
    )
    dfa = dfa.drop(
        columns=[
            "officer_name",
            "race",
            "sex",
            "age",
            "years_of_service",
            "use_of_force_description",
            "use_of_force_level",
            "use_of_force_effective",
            "officer_injured",
            "agency",
        ]
    )
    df = pd.merge(dfa, dfb, on="uof_uid")
    return df


def extract_citizen(uof):
    df = (
        uof.loc[
            :,
            [
                "citizen_race",
                "citizen_sex",
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_influencing_factors",
                "citizen_distance_from_officer",
                "citizen_age",
                "citizen_build",
                "citizen_height",
                "citizen_arrested",
                "citizen_arrest_charges",
                "agency",
                "uof_uid",
            ],
        ]
        .pipe(clean_column_names)
        .pipe(split_citizen_rows)
        .pipe(clean_citizen_arrest_charges)
        .pipe(clean_citizen_age)
        .pipe(clean_citizen_cols)
        .pipe(clean_races, ["citizen_race"])
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(
            standardize_desc_cols,
            [
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_distance_from_officer",
                "citizen_age",
                "citizen_arrested",
                "citizen_influencing_factors",
                "citizen_build",
                "citizen_height",
            ],
        )
        .pipe(set_values, {"agency": "new-orleans-pd"})
        .pipe(
            gen_uid,
            [
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_distance_from_officer",
                "citizen_age",
                "citizen_arrested",
                "citizen_influencing_factors",
                "citizen_build",
                "citizen_height",
                "agency",
            ],
            "citizen_uid",
        )
    )
    uof = uof.drop(
        columns=[
            "citizen_race",
            "citizen_sex",
            "citizen_hospitalized",
            "citizen_injured",
            "citizen_distance_from_officer",
            "citizen_arrested",
            "citizen_arrest_charges",
            "citizen_influencing_factors",
            "citizen_build",
            "citizen_age",
            "citizen_height",
        ]
    )
    return df, uof


def strip_col_prefix(df):
    df.columns = df.columns.str.replace(r"(inc_|uof_|off_|org_)", "", regex=True)
    return df


def clean_department_desc_22(df):
    df.loc[:, "department_desc"] = df.bureau.str.lower().str.strip().str.replace(r"^(\w+) - ", "", regex=True)
    return df.drop(columns=["bureau"])


def clean_uof_22():
    df = (pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pd_uof_2022.csv"))
          .pipe(clean_column_names)
          .pipe(strip_col_prefix)
          .drop(columns=["fd_discharge_type"])
          .rename(columns={"pib_control_number": "tracking_id_og", 
                           "division_assignment": "division", 
                           "unit_assignment": "unit",
                           "light_conditions": "light_condition", 
                           "type_of_force_used": "use_of_force_description",
                           "citizen_condition_injury": "citizen_injury_description",
                           "effective_y_n": "use_of_force_effective", 
                           "gender": "sex",
                           "citizen_s_distance_from_officer_employee": "citizen_distance_from_officer",
                           "cit_gender": "citizen_sex", 
                           "cit_race": "citizen_race", 
                           "citizen_s_build": "citizen_build", 
                           "citizen_s_height": "citizen_height",
                           "citizen_was_injured_y_n": "citizen_injured", 
                           "citizen_went_to_hospital_y_n": "citizen_hospitalized",
                           "citizen_was_arrested_y_n": "citizen_arrested", 
                           "cit_charge": "citizen_influencing_factors", 
                           "reason_for_using_force": "use_of_force_reason", 
                           "officer_employee_was_injured_y_n": "officer_injured",
                           "occurred_date": "uof_occur_date"})
          .pipe(clean_department_desc_22)
          .pipe(clean_disposition)
          .pipe(clean_use_of_force_description)
          .pipe(clean_names, ["first_name", "last_name"])
          .pipe(clean_sexes, ["sex", "citizen_sex"])
          .pipe(clean_races, ["race", "citizen_race"])
          .pipe(clean_dates, ["hire_date", "uof_occur_date"])
          .pipe(standardize_desc_cols, ["division", "unit", "shift_hours", "status", 
                                        "service_rendered", "light_condition", "weather_condition",
                                        "citizen_injury_description", "use_of_force_effective",
                                        "citizen_distance_from_officer", "citizen_build", "citizen_height",
                                        "officer_injured", "tracking_id_og", "working_status",
                                        "citizen_influencing_factors", "use_of_force_reason", 
                                        "citizen_arrested", "citizen_hospitalized", "citizen_injured",
                                        "citizen_hospitalized"])
          .pipe(extract_use_of_force_level)
          .pipe(set_values, {"agency": "new-orleans-pd"})
          .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["use_of_force_level", "use_of_force_description", "use_of_force_reason", 
                          "uid", "use_of_force_effective", "tracking_id", "use_of_force_effective",
                          'status', 'disposition', 'service_rendered', 'light_condition',
                          'weather_condition', 'division', 'unit', 'working_status', 'shift_hours', "citizen_influencing_factors"], "uof_uid")
    )
    return df 


def extract_citizen_22(uof):
    df = (
        uof.loc[
            :,
            [
                "citizen_race",
                "citizen_sex",
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_influencing_factors",
                "citizen_distance_from_officer",
                "citizen_build",
                "citizen_height",
                "citizen_arrested",
                "agency",
                "uof_uid",
            ],
        ]
        .pipe(
            gen_uid,
            [
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_distance_from_officer",
                "citizen_arrested",
                "citizen_influencing_factors",
                "citizen_build",
                "citizen_height",
                "agency",
            ],
            "citizen_uid",
        )
    )
    uof = uof.drop(
        columns=[
                "citizen_race",
                "citizen_sex",
                "citizen_hospitalized",
                "citizen_injured",
                "citizen_influencing_factors",
                "citizen_distance_from_officer",
                "citizen_build",
                "citizen_height",
                "citizen_arrested",
        ]
    )
    return df, uof


def concat_dfs(uof, uof22, uof_citizen, uof_citizen_22):
    uof = pd.concat([uof, uof22], axis=0)
    uof_citizen = pd.concat([uof_citizen, uof_citizen_22], axis=0)

    uof = uof.drop_duplicates(subset=["uid", "uof_uid"])
    uof_citizen = uof_citizen.drop_duplicates(subset=["citizen_uid", "uof_uid"])
    return uof, uof_citizen


if __name__ == "__main__":
    uof = clean_uof()
    uof_citizen, uof = extract_citizen(uof)
    uof_22 = clean_uof_22()
    uof_citizen_22, uof_22 = extract_citizen_22(uof_22)
    uof, uof_citizen = concat_dfs(uof, uof_22, uof_citizen, uof_citizen_22)
    uof.to_csv(deba.data("clean/uof_new_orleans_pd_2016_2022.csv"), index=False)
    uof_citizen.to_csv(
        deba.data("clean/uof_citizens_new_orleans_pd_2016_2022.csv"), index=False
    )

from lib.columns import clean_column_names
import deba
from lib.clean import clean_names, standardize_desc_cols, clean_dates, clean_sexes
from lib.uid import gen_uid
import pandas as pd


def assign_agency(df):
    df.loc[:, "data_production_year"] = 2020
    df.loc[:, "agency"] = "kenner-pd"
    return df


def split_names(df):
    df.loc[:, "name"] = df.name.str.strip()
    names = (
        df.name.str.replace(r", ?(Jr\.|III),?", r" \1,", regex=True)
        .str.lower()
        .str.replace(r" (\w)\.", r" \1")
        .str.extract(r"^([^,]+), (jo ann|[\w-]+)(?: (.+))?$")
    )
    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    df = df.loc[(df.name != "unknown") & df.name.notna()]
    # there's already a "Gregory, Jan-Michael" so this row is probably redundant
    df = df.loc[df.name != "gregory, Jan Michael"]
    df.drop(columns=["name"])
    return df.reset_index(drop=True)


def clean():
    return (
        pd.read_csv(deba.data("raw/kenner_pd/kenner_pd_pprr_2020.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "id_no": "employee_id",
                "rank": "rank_desc",
                "division": "department_desc",
                "date_hired": "hire_date",
                "work_classification": "employment_status",
                "status": "officer_inactive",
                "gender": "sex",
            }
        )
        .drop(columns=["years_with_dept"])
        .pipe(clean_employee_id)
        .pipe(split_names)
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(
            standardize_desc_cols,
            [
                "sex",
                "department_desc",
                "rank_desc",
                "employment_status",
                "officer_inactive",
                "sworn",
            ],
        )
        .pipe(remove_non_officers)
        .pipe(clean_dates, ["hire_date"])
        .pipe(clean_rank)
    )


def drop_volunteers(df):
    df = df[df.employee_class != "volunteer"]
    return df.reset_index(drop=True)


def clean_employee_id(df):
    df.loc[:, "employee_id"] = df.employee_id.str.strip()
    return df


def remove_non_officers(df):
    return df[
        ~df.rank_desc.str.match(
            r"^(mr|ms|mrs|call taker|father|chaplain|custodian|explorer|not stated)\.?$"
        )
    ].reset_index(drop=True)


def clean_rank(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.replace(r" off(er|c)?$", " officer", regex=True)
        .str.replace(r" tech$", " technician", regex=True)
        .str.replace(r"admin\.", "administrative", regex=True)
        .str.replace(r"dir\. of", "director of", regex=True)
        .str.replace(r"coll\.", "collector", regex=True)
        .str.replace(r"invest.", "investigator", regex=True)
        .str.replace(r" prop\.", " property", regex=True)
        .str.replace(r" (\bi[il]?\b)", "", regex=True)
        .str.replace(r" maint$", " maintainer", regex=True)
        .str.replace("sergeant", "sargeant", regex=False)
        .str.replace(r" - ", " ", regex=False)
        .str.replace("chief's secretary", "secretary to the chief", regex=False)
        .str.replace(r"super$", "superintendent", regex=True)
        .str.replace("emerg mgm speci", "emergency mgm specialist", regex=False)
        .str.replace(r"\bco\b", "coordinator", regex=True)
    )
    return df


def clean_former_long():
    return (
        pd.read_csv(deba.data("raw/kenner_pd/kenner_pd_pprr_formeremployees_long.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "title": "rank_desc",
                "lnam": "last_name",
                "fnam": "first_name",
                "hired": "hire_date",
                "compid": "employee_id",
                "classifica": "employee_class",
            }
        )
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(standardize_desc_cols, ["rank_desc", "employee_class"])
        .pipe(remove_non_officers)
        .pipe(clean_employee_id)
        .pipe(clean_dates, ["hire_date"])
    )


def clean_former_short():
    return (
        pd.read_csv(deba.data("raw/kenner_pd/kenner_pd_pprr_formeremployees_short.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date_hired": "hire_date",
                "date_left": "left_date",
                "division": "department_desc",
                "rank": "rank_desc",
                "id_no": "employee_id",
                "gender": "sex",
            }
        )
        .pipe(clean_employee_id)
        .drop(columns=["gone", "reason_left"])
        .pipe(assign_agency)
        .pipe(split_names)
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(standardize_desc_cols, ["department_desc", "rank_desc", "sex"])
        .pipe(remove_non_officers)
        .pipe(clean_dates, ["hire_date", "left_date"])
    )


def combine_pprrs(pprr, former_long, former_short):
    records = pprr.set_index("employee_id", drop=False).to_dict("index")
    for other_df in [former_long, former_short]:
        for idx, row in other_df.set_index("employee_id", drop=False).iterrows():
            if idx in records:
                record = records[idx]
                for k, v in row.to_dict().items():
                    if v == "" or pd.isnull(v):
                        continue
                    if k in record and pd.notnull(record[k]) and record[k] != "":
                        continue
                    record[k] = v
            else:
                records[idx] = row.to_dict()
    return (
        pd.DataFrame.from_records(list(records.values()))
        .pipe(assign_agency)
        .pipe(clean_rank)
        .pipe(gen_uid, ["agency", "employee_id"])
    )

def clean_date(df: pd.DataFrame, cols: list[str], expand: bool = True) -> pd.DataFrame:
    for col in cols:
        assert col.endswith("_date"), f"Expected column '{col}' to end with '_date'"

        cleaned = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace("//", "/", regex=False)
            .str.replace("'", "", regex=False)
        )

        parsed = pd.to_datetime(cleaned, errors="coerce")

        if expand:
            prefix = col[:-5]
            df[f"{prefix}_year"] = parsed.dt.year
            df[f"{prefix}_month"] = parsed.dt.month
            df[f"{prefix}_day"] = parsed.dt.day
            df = df.drop(columns=[col])
        else:
            df[col] = parsed

    return df

def strip_leading_commas_apostrophes(df: pd.DataFrame) -> pd.DataFrame:
    def clean_cell(val):
        if isinstance(val, str):
            return val.lstrip("', ").strip()
        return val
    return df.applymap(clean_cell)

def clean_25():
    return (
        pd.read_csv(deba.data("raw/kenner_pd/kenner_pd_pprr_1990_2001.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "id_no": "employee_id",
                 "rank": "rank_desc",
                 "division": "department_desc",
                 "date_hired": "hire_date",
                 "work_classification": "employment_status",
                 "status": "officer_inactive",
                 "gender": "sex",
             }
         )
        .drop(columns=["years_with_dept"])
        .pipe(clean_employee_id)
        .pipe(split_names)
        .drop(columns=["name"])
        .pipe(clean_names, ["first_name", "last_name", "middle_name"])
        .pipe(
            standardize_desc_cols,
            [
                "sex",
                "department_desc",
                "rank_desc",
                "employment_status",
                "officer_inactive",
                "sworn",
            ],
        )
        .pipe(clean_date, ["hire_date"])
        .pipe(clean_rank)
        .pipe(clean_sexes, ["sex"])
        .pipe(strip_leading_commas_apostrophes)
        .pipe(assign_agency)
        .pipe(gen_uid, ["agency", "employee_id"])
    )

def combine_dedup_pprrs(new, old):
    new["match_key"] = (
        new["employee_id"].astype(str).str.strip().str.lower() 
    )

    old["match_key"] = (
        old["employee_id"].astype(str).str.strip().str.lower()
    )

    old_filtered = old[~old["match_key"].isin(new["match_key"])]
    combined = pd.concat([new, old_filtered], ignore_index=True)
    return combined.drop(columns=["match_key"])


if __name__ == "__main__":
    pprr = clean()
    pprr_25 = clean_25()
    former_long = clean_former_long()
    former_short = clean_former_short()
    combined = combine_pprrs(pprr, former_long, former_short)
    pprr_2025_combined = combine_dedup_pprrs(combined, pprr_25)
    combined.to_csv(deba.data("clean/pprr_kenner_pd_2020.csv"), index=False)
    pprr_2025_combined.to_csv(deba.data("clean/pprr_kenner_pd_2025.csv"), index=False)

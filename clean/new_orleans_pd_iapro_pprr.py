import deba
from lib.columns import clean_column_names
from lib.clean import (
    clean_races,
    float_to_int_str,
    standardize_desc_cols,
    clean_sexes,
    clean_names,
    clean_dates,
)
from lib.uid import gen_uid
from lib.standardize import standardize_from_lookup_table
import pandas as pd


def remove_badge_number_zeroes_prefix(df):
    df.loc[:, "badge_no"] = df.badge_no.str.replace(r"^0+", "", regex=True)
    return df


def clean_employee_type(df):
    df.loc[:, "employee_type"] = df.employee_type.str.lower().str.replace(
        r"commisioned", "commissioned"
    )
    return df


def strip_time_from_dates(df, cols):
    for col in cols:
        df.loc[:, col] = (
            df[col]
            .str.replace(r" \d+:.+", "", regex=True)
            .str.replace(r"(\d{4})-(\d{2})-(\d{2})", r"\2/\3/\1", regex=True)
        )
    return df


def assign_agency(df):
    df.loc[:, "data_production_year"] = "2018"
    df.loc[:, "agency"] = "new-orleans-pd"
    return df


def clean_current_supervisor(df):
    df.loc[:, "officer_primary_key"] = df.officer_primary_key.astype(str)
    officer_number_dict = df.set_index("officer_primary_key").uid.to_dict()
    df.loc[:, "current_supervisor"] = df.current_supervisor.map(
        lambda x: officer_number_dict.get(x, "")
    )
    return df


def remove_unnamed_officers(df):
    df.loc[:, "last_name"] = (
        df.last_name.fillna("")
        .str.replace(r"^unknown.*", "", regex=True)
        .str.replace(r"^none$", "", regex=True)
        .str.replace("not an nopd officer", "", regex=False)
        .str.replace(r"^anonymous$", "", regex=True)
        .str.replace(r"^known$", "", regex=True)
        .str.replace(r"^unkown$", "", regex=True)
        .str.replace(r"^delete$", "", regex=True)
        .str.replace(r"^nopd$", "", regex=True)
        .str.replace(r"^not nopd$", "", regex=True)
    )
    return df[df.last_name != ""].reset_index(drop=True)


def clean_department_desc(df):
    df.department_desc = (
        df.department_desc.str.lower()
        .str.strip()
        .str.replace(r"(fob|isb|msb|pib|not) - ", "", regex=True)
        .str.replace(r"\bservice\b", "services", regex=True)
        .str.replace("nopd officer", "", regex=False)
    )
    return df


def clean_rank_desc(df):
    df.rank_desc = (
        df.rank_desc.str.lower()
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(r" ?investigative special$", "", regex=True)
        .str.replace(r" ?police", "", regex=True)
        .str.replace(r"dec$", "decree", regex=True)
        .str.replace(r"supt", "superintendent", regex=False)
        .str.replace(r"\bdev(e)?\b", "developer", regex=True)
        .str.replace(",", " ", regex=False)
        .str.replace(r"iv$", "", regex=True)
        .str.replace(r" ?-", " ", regex=True)
        .str.replace(r"(ii?i?|1|2|3|4)?$", "", regex=True)
        .str.replace(r"spec$", "specialist", regex=True)
        .str.replace(r"sup(v)?$", "supervisor", regex=True)
        .str.replace(r"\basst\b", "assistant", regex=True)
        .str.replace(" ?sr", "senior", regex=True)
        .str.replace(r" ?mgr", " manager", regex=True)
        .str.replace(" academy", "", regex=False)
        .str.replace(r" \boff\b ?", " officer", regex=True)
        .str.replace(r" of$", "", regex=True)
        .str.replace(r"(3|4|&|5)", "", regex=True)
        .str.replace(r" \bcoor\b", " coordinator", regex=True)
        .str.replace(r"\bopr\b", "operations", regex=True)
        .str.replace("default", "", regex=False)
        .str.replace(r"\bspec\b", "specialist", regex=True)
        .str.replace("recov", "recovery", regex=False)
        .str.replace(r"\bprog\b", "program", regex=True)
        .str.replace(r"\btech\b", "technician", regex=True)
        .str.replace("applic", "application", regex=False)
        .str.replace(r" \(nopd\)$", "", regex=True)
        .str.replace("cnslr", "counseler", regex=False)
        .str.replace(r"\binfo\b", "information,", regex=True)
        .str.replace("  ", " ", regex=False)
        .str.replace(r"awards coord \( dept\)", "awards coordinator", regex=True)
        .str.replace(r"information?\,?", "information", regex=True)
        .str.replace(r"\binstru\b", "instructor", regex=True)
        .str.replace(r"\badmini?n?s?t?r?a?t?i?v?e?\b", "admin", regex=True)
        .str.replace(r"\bcouns\b", "counselor", regex=True)
        .str.replace(r"\bfield\b", "", regex=True)
        .str.replace(r"\(eis\)", "", regex=True)
        .str.replace(r"\banalyt?\b", "analyst", regex=True)
        .str.replace(r"\bse$", "", regex=True)
        .str.replace(r"\bapp inv a\b", "", regex=True)
        .str.replace(r" \bex\b ", "", regex=True)
        .str.replace(r"\bsup sup\b", "", regex=True)
        .str.replace(r"operations$", "operator", regex=True)
        .str.replace(r"^unknown rank$", "", regex=True)
        .str.replace(r"^dna analyst senior$", "senior dna analyst", regex=True)
    )
    return df


def clean_sub_division_b_desc(df):
    df.loc[:, "sub_division_b_desc"] = (
        df.sub_division_b_desc.str.lower()
        .str.strip()
        .str.replace(r"^not - nopd officer$", "", regex=True)
        .str.replace(r"^unknown ?(assignment)?", "", regex=True)
        .str.replace(r"^(.+) platoon (\w{1})$", r"\1; platoon \2", regex=True)
        .str.replace(r"^(.+) squad (\w{1})$", r"\1; squad \2", regex=True)
        .str.replace(r"^admin unit$", "administrative unit", regex=True)
        .str.replace(r"v\.o\.w\.s\.", "violent offender warrant squad", regex=True)
        .str.replace(r"squad unit$", "squad", regex=True)
        .str.replace(r"^district$", "", regex=True)
        .str.replace(r"^detailed officers$", "", regex=True)
    )
    return df


def clean():
    df = pd.read_csv(deba.data("raw/new_orleans_pd/new_orleans_pprr_1946_2018.csv"))
    df = df.dropna(axis=1, how="all")
    df = clean_column_names(df)
    print(df.columns.tolist())
    df = df.drop(
        columns=["employment_number", "working_status", "shift_hours", "exclude_reason"]
    )
    df = df.rename(
        columns={
            "badge_number": "badge_no",
            "title": "rank_desc",
            "employment_ended_on": "left_date",
            "officer_department": "department_desc",
            "officer_division": "division_desc",
            "officer_sub_division_a": "sub_division_a_desc",
            "officer_sub_division_b": "sub_division_b_desc",
            "assigned_to_unit_on": "dept_date",
            "hired_on": "hire_date",
            "officer_sex": "sex",
            "officer_race": "race",
            "middle_initial": "middle_name",
            "officer_number": "officer_primary_key",
        }
    )
    return (
        df.pipe(
            float_to_int_str, ["years_employed", "current_supervisor", "birth_year"]
        )
        .pipe(remove_badge_number_zeroes_prefix)
        .pipe(clean_rank_desc)
        .pipe(
            standardize_desc_cols,
            [
                "rank_desc",
                "employment_status",
                "officer_inactive",
                "department_desc",
                "division_desc",
                "sub_division_a_desc",
            ],
        )
        .pipe(clean_employee_type)
        .pipe(clean_sexes, ["sex"])
        .pipe(clean_races, ["race"])
        .pipe(clean_department_desc)
        .pipe(assign_agency)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(
            gen_uid, ["agency", "first_name", "last_name"]
        )
        .pipe(strip_time_from_dates, ["hire_date", "left_date", "dept_date"])
        .pipe(clean_dates, ["hire_date", "left_date", "dept_date"])
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(remove_unnamed_officers)
        .pipe(clean_current_supervisor)
        .pipe(
            standardize_from_lookup_table,
            "division_desc",
            [
                ["eighth district"],
                ["seventh district"],
                ["sixth district"],
                ["fifth district"],
                ["fourth district"],
                ["third district"],
                ["second district"],
                ["first district"],
                ["budget services", "superintendant's budget staff"],
                ["recruitment", "police recruits"],
                ["special operations", "special operations division"],
                ["administration", "administrative duties services"],
                ["staff", "staff division", "staff programs unit"],
                ["communications", "communications division"],
                [
                    "special investigations",
                    "special investigation section",
                    "special investigations division",
                ],
                ["reserves", "reserve division"],
                [
                    "education, training, recruitment",
                    "education/training & recruitment division",
                ],
                [
                    "records, identification, support",
                    "records & identification / support services divisi",
                ],
                ["crime lab", "crime lab and evidence division"],
                ["intake", "intake unit"],
                ["consent decree"],
                [
                    "criminal investigations",
                    "criminal investigations division",
                    "criminal investigation section",
                ],
                ["technology", "technology section"],
                ["alternative police response"],
                [
                    "administrative investigations",
                    "administrative investigation section",
                ],
                ["force investigations", "force investigation team"],
                ["command", "command staff"],
                ["special events"],
                ["public information office"],
                ["police", "polpln policy planning"],
                ["i.s.b", "isb"],
                ["office of the superintendant"],
            ],
        )
        .pipe(
            standardize_from_lookup_table,
            "sub_division_a_desc",
            [
                ["staff"],
                ["personnel", "personnel division"],
                ["workmans compensation"],
                ["traffic", "traffic section"],
                ["platoon 2", "b platoon", "2nd platoon"],
                ["platoon 1", "a platoon", "1st platoon"],
                ["platoon 3", "c platoon", "3rd platoon"],
                ["investigations"],
                ["d.i.u"],
                ["i.o.d"],
                ["i.s.b", "isb"],
                ["l.w.o.p", "lwop"],
                ["fiscal", "fiscal / criminal justice"],
                ["special investigations", "special investigation section"],
                ["training", "training section"],
                ["records", "records & i.d."],
                ["night beat", "night watch"],
                ["day beat", "day watch", "day beats"],
                ["evening beat", "evening watch"],
                ["k9", "k9 section"],
                [
                    "criminal investigations",
                    "scientific criminal investigations section",
                    "criminal investigation section",
                ],
                ["professional standards", "professional standards section"],
                ["support", "support services"],
                ["tactical", "tactical section"],
                ["narcotics", "narcotics section"],
                ["street gang", "street gang unit"],
                [
                    "central evidence and property section",
                    "central evidence & property section",
                ],
                ["intelligence", "intelligence section"],
                ["juvenile", "juvenile section"],
                ["task force"],
                ["school resources", "school resource officers"],
                ["multi-agency gang unit"],
                ["mayor's security", "mayors security"],
                ["military leave"],
                ["recruitment", "recruitment division"],
                [
                    "administrative investigations",
                    "administrative investigation section",
                ],
                ["special victims", "special victim  section"],
                ["force investigations", "force investigation team"],
                ["homicide", "homicide section"],
                ["administration", "admin"],
                ["property crimes", "property crimes section"],
                ["lost property"],
                ["task force a"],
                ["staff", "staff programs unit"],
                ["public affairs"],
                ["building security"],
                [
                    "crime prevention",
                    "crime prevention section",
                    "crime prevention/quality of life",
                ],
                ["court liason", "court liaison"],
                ["victim/witness assistance", "victim/witness assistance unit"],
                ["neighborhood watch", "neighborhood policing"],
                ["criminal investigations staff", "cid staff"],
                ["homeland security"],
                ["crime analysis"],
                ["grants", "grants administration section"],
                ["training", "training platoon"],
                ["magazine street patrol", "magazine st. patrol"],
                ["homeless assistance"],
                ["lakeview crime prevention"],
                ["french market patrol"],
                ["mid-city patrol", "mid-city"],
                ["compliance", "compliance section"],
                ["8th district"],
                ["5th district"],
                ["district attorney", "district attorney section"],
                ["communications"],
                ["mounted"],
                ["3rd district"],
                ["bourbon promenade"],
                ["6th district"],
                ["quality of life"],
                ["crime lab", "crime lab section"],
                ["opse"],
                ["fleet equipment services", "fleet & equipment services section"],
            ],
        )
        .pipe(clean_sub_division_b_desc)
        .drop_duplicates(subset=["uid"], keep="first")
    )


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/pprr_new_orleans_pd_1946_2018.csv"), index=False)

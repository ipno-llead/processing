import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.uid import gen_uid
from lib.clean import standardize_from_lookup_table, strip_leading_comma, clean_salaries


dept_desc_lookup_table = [
    ["it", "i.t."],
    ["accounting"],
    ["payroll"],
    ["purchasing"],
    ["narcotics"],
    ["baliff"],
    ["intake"],
    ["jail"],
    ["communications"],
    ["property", "propert & evidence"],
    ["cib"],
    ["fines"],
    ["internal affairs"],
    ["burglary"],
    ["central records"],
    ["crime lab"],
    ["emergency medical services", "ems"],
    ["homicide"],
    ["robbery"],
    ["internal management"],
    ["jtff"],
    ["ncic"],
    ["task force"],
    ["management"],
    ["pawn shop"],
    ["auto theft"],
    ["arson"],
    ["div c"],
    ["div b"],
    ["div a"],
    ["it; network", "network"],
    ["it; database", "database"],
    ["mail"],
    ["academy", "training"],
    ["accounting"],
    ["human resources"],
    ["court"],
    ["strategic engagement"],
    ["narcotics", "narcoces", "narco", "narocites", "narcoitcs"],
    ["dea"],
    ["transportation"],
    ["jefferson parish corrections center", "jpcc", "jail management"],
    ["booking"],
    ["s.i.u"],
    ["shoot squaf"],
    ["economic crime"],
    ["traffic"],
    ["firearms"],
    ["crime scene"],
    ["dna lab", "dna"],
    ["tax supervisor"],
    ["tax collecor"],
    ["finance", "financial"],
    ["budget"],
    ["grants"],
    ["health insurance"],
    ["sales tax"],
    ["operations"],
    ["missing persons"],
    ["crimes"],
    ["personal violence"],
    ["intenstive probation"],
    ["intelligence"],
    ["cic"],
    ["management services", "manag services"],
    ["print shop"],
    ["help desk tech"],
    ["light shop"],
    ["range"],
    ["accounts payable"],
    ["school offense"],
    ["p.o.c"],
    ["sib"],
    ["technical services", "tech services"],
    ["dlr"],
    ["asset forfeiture", "asset forfeure"],
    ["prisoner"],
    ["technical services"],
    ["lab services"],
    ["community relations"],
    ["youth programs"],
    ["public assignments"],
    ["patrol"],
    ["digital forensic"],
    ["evidence"],
    ["polygraph"],
    ["fingerprint"],
    ["photo lab"],
    ["vice"],
    ["insurance"],
    ["handgun permits", "handgun perms"],
    ["terminal agency"],
    ["personnel background"],
    ["forensic chemistry"],
    ["process server"],
    ["intensive probation"],
    ["shoot squad"],
    ["qa/qc"],
    ["commisary"],
    ["canine"],
    ["paint and body"],
    ["fbi"],
    ["24th juidicial - i"],
    ["24th judicial - e"],
    ["24th judicial - f"],
    ["5th circuit app"],
    ["fbi task force"],
    ["24th judicial - j"],
    ["24th judicial - a"],
    ["24th judicial - n"],
    ["24th judicial - o"],
    ["24th judicial - c"],
    ["24th judicial - l"],
    ["communications ct. - domestic", "comm ct. - domestic"],
    ["24th judicial - g"],
    ["intake booking"],
    ["juvenile court division a", "juvenile court baliff, div. a"],
    ["fleet management"],
    ["juvenile court division c", "juvenile court baliff, div. c"],
    ["campus police", "police on campus"],
    ["jttf"],
    ["division a", "div. a"],
    ["division b", "div. b"],
    ["2nd parish ct"],
    ["codis"],
    ["ions"],
    ["it; motor pool", "motor pool", "eb motor pool"],
    ["yenni building"],
    ["division c", "div. c"],
    ["24th juidicial h"],
    ["24th juidicial - m"],
    ["inmate security", "inmate sec."],
    ["claims"],
    ["contracts office", "contracts manager office"],
    ["24th judicial - h"],
    ["prisoner transportation"],
    ["parish security"],
]


def extract_department_descriptions(df):
    df.loc[:, "department_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" $", "", regex=True)
        .str.replace(r"   +", " ", regex=True)
    )
    return standardize_from_lookup_table(df, "department_desc", dept_desc_lookup_table)


def clean_department_descriptions(df):
    df.loc[:, "department_desc"] = (
        df.department_desc.str.replace(r"^it; it$", "it", regex=True)
        .str.replace(r"^patrol; patrol$", "patrol", regex=True)
        .str.replace(r"^it; it; network", "it; network", regex=True)
    )
    return df


def clean_district_desc(df):
    df.loc[:, "district"] = (
        df.dept_desc.fillna("")
        .str.replace(r" ?sergeant ?", "", regex=True)
        .str.replace("lieutenant", "", regex=False)
    )
    return df.drop(columns="dept_desc")


def split_names(df):
    names = (
        df.name.fillna("")
        .str.replace(r"\.\,", ",", regex=True)
        .str.replace("sue ellen", "sue-ellen", regex=False)
        .str.replace("jon' janice", "jon'janice", regex=False)
        .str.replace("photo lab day", "photolabday", regex=False)
        .str.replace(" employees", "", regex=False)
        .str.extract(
            r"^(\w+\-?\.?\'? ?\w+?\'?) ?(?:(\w+) )?\, (?:(\w+\-?\'?\w+?\'?)) ?(\w+)?\.?$"
        )
    )

    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "suffix"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2].fillna("")
    df.loc[:, "middle_name"] = names[3].fillna("").str.replace(r"\.", "", regex=True)
    df.loc[:, "last_name"] = df.last_name.str.cat(df.suffix, sep=" ")
    return df[~((df.first_name == "") & (df.last_name == ""))].drop(
        columns=["suffix", "name"]
    )


rank_desc_lookup_table = [
    [
        "commander",
        "commaander",
        "enforcement comn",
        "comma",
        "prisoner transportation com",
        "commai",
        "commando",
    ],
    ["custodian", "custodia"],
    ["detective", "detecti", "detectiv"],
    ["officer"],
    ["deputy"],
    ["deputy field trainee"],
    ["supervisor", "supervis", "super", "superv"],
    ["manager"],
    ["chief officer"],
    ["chemist"],
    ["investigator"],
    ["assistant", "asst."],
    ["systems specialist", "systspeciali"],
    ["emrgency computer", "emergencycomputer"],
    ["sergeant", "serge", "sergeant; sergeant"],
    ["coordinator"],
    ["systems manager", "systmanager"],
    ["bailiff", "baliff"],
    ["field trainee"],
    ["instructor"],
    ["clerk"],
    ["chief pilot"],
    ["legal liaison"],
    ["chief"],
    ["mechanic"],
    ["enforcement commander", "enforcement comn"],
    ["dispatcher"],
    ["chief's secretary", "chief's secret"],
    ["administrator", "admin"],
    ["sheriff"],
    ["chaplain"],
    ["polygraphist"],
    ["analyst"],
    ["technician"],
    ["director"],
    ["lieutenant"],
    ["community liaison"],
    ["repairman"],
    ["campus police", "police campus"],
    ["grants writer"],
    ["secretary"],
]


def standardize_rank_descriptions(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.lower()
        .str.strip()
        .fillna("")
        .str.replace(r" $", "", regex=True)
        .str.replace(r"  +", " ", regex=True)
    )
    return standardize_from_lookup_table(df, "rank_desc", rank_desc_lookup_table)


def clean_rank_descriptions(df):
    df.loc[:, "rank_desc"] = (
        df.rank_desc.str.replace(r"^sergeant; sergeant$", "sergeant", regex=True)
        .str.replace(r"^custodian; custodian", "custodian", regex=True)
        .str.replace(r"^detective; detective", "detective", regex=True)
        .str.replace(
            r"^deputy; deputy; field trainee$", "deputy; field trainee", regex=False
        )
        .str.replace(r"^deputy; deputy", "deputy", regex=True)
        .str.replace(r"^technician; technician", "technician", regex=True)
        .str.replace(r"^chief; officer$", "chief", regex=True)
        .str.replace(r"repairman; repairman", "repairman", regex=True)
    )
    return df


def drop_rows_missing_name(df):
    return df[~((df.first_name.fillna("") == ""))]


def clean():
    df = (
        pd.read_csv(deba.data("raw/jefferson_so/jefferson_parish_so_pprr_2020.csv"))
        .pipe(clean_column_names)
        .pipe(clean_district_desc)
        .pipe(split_names)
        .pipe(extract_department_descriptions)
        .pipe(clean_department_descriptions)
        .pipe(standardize_rank_descriptions)
        .pipe(clean_rank_descriptions)
        .pipe(set_values, {"agency": "jefferson-so"})
        .pipe(
            gen_uid, ["agency", "first_name", "middle_name", "last_name", "employee_id"]
        )
        .pipe(drop_rows_missing_name)
    )
    return df


def split_name_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Splits 'name' column in format 'Last, First Middle'
    into lowercase first_name, middle_name, last_name.
    Drops the original 'name' column.
    """
    def split_name(name: str):
        if pd.isna(name):
            return pd.Series(["", "", ""])
        
        # split into last and the rest
        if "," in name:
            last, rest = name.split(",", 1)
            parts = rest.strip().split()
            first = parts[0] if len(parts) > 0 else ""
            middle = " ".join(parts[1:]) if len(parts) > 1 else ""
        else:
            # fallback if no comma
            parts = name.split()
            first = parts[0] if len(parts) > 0 else ""
            middle = " ".join(parts[1:-1]) if len(parts) > 2 else ""
            last = parts[-1] if len(parts) > 1 else ""
        
        return pd.Series([first.lower(), middle.lower(), last.lower()])

    df[["first_name", "middle_name", "last_name"]] = df["name"].apply(split_name)
    return df.drop(columns=["name"])

def split_hire_date(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extracts hire_year, hire_month, hire_day from 'date_of_hire' regardless of
    stray characters. Expects a YYYYMMDD somewhere in the cell.
    Non-matching rows become <NA>. Drops original 'date_of_hire'.
    """
    # ensure string
    s = df["date_of_hire"].astype(str).str.strip()

    # extract strictly YYYY MM DD anywhere in the string
    parts = s.str.extract(r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})")

    # coerce to numbers with nullable Int64
    df["hire_year"]  = pd.to_numeric(parts["year"], errors="coerce").astype("Int64")
    df["hire_month"] = pd.to_numeric(parts["month"], errors="coerce").astype("Int64")
    df["hire_day"]   = pd.to_numeric(parts["day"], errors="coerce").astype("Int64")

    # optional: zero out impossible dates like 0000-00-00 -> <NA>
    mask_zero = (df["hire_year"] == 0) | (df["hire_month"] == 0) | (df["hire_day"] == 0)
    df.loc[mask_zero, ["hire_year", "hire_month", "hire_day"]] = pd.NA

    # drop original column
    return df.drop(columns=["date_of_hire"])

import pandas as pd

def clean_rank_jpso(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize JPSO rank codes to lowercase full words.
    Known ranks are expanded; uncertain values (CVxx, numbers, 'Civil', blanks, 'LE')
    are set to <NA>. Rewrites the 'rank' column in-place.
    """
    s = df["rank"].astype("string")

    norm = (
        s.str.strip()
         .str.upper()
         .str.replace(".", "", regex=False)  # 'Lt.' -> 'LT'
    )

    known_map = {
        "DEP":  "deputy",
        "SGT":  "sergeant",
        "LT":   "lieutenant",
        "DET":  "detective",
        "CAPT": "captain",
        "MAJ":  "major",
        "SHER": "sheriff",
        "CH":   "chief",
    }

    out = norm.map(known_map)

    uncertain = (
        norm.isna()
        | (norm == "")
        | norm.str.fullmatch(r"CV\d+")   # CV25, CV41, CV44
        | norm.str.contains(r"/")        # 248/274
        | norm.str.fullmatch(r"\d+")     # 121
        | norm.isin(["CIVIL", "LE"])     # division/ambiguous
    )

    out = out.mask(uncertain, '')

    df["rank"] = out
    return df

def clean_25():
    df = (
        pd.read_csv(deba.data("raw/jefferson_so/jpso_pprr_2025.csv"))
        .pipe(clean_column_names)
        .pipe(strip_leading_comma)
        .pipe(split_name_columns)
        .pipe(split_hire_date)
        .pipe(clean_rank_jpso)
        .pipe(set_values, {"agency": "jefferson-so", "salary_freq": "annual"})
        .pipe(
            gen_uid, ["agency", "first_name", "middle_name", "last_name"]
        )
    )
    return df


if __name__ == "__main__":
    df = clean()
    df25 = clean_25()
    df.to_csv(deba.data("clean/pprr_jefferson_so_2020.csv"), index=False)
    df25.to_csv(deba.data("clean/pprr_jefferson_so_2025.csv"), index=False)

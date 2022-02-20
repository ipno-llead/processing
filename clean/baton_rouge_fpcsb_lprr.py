import bolo
from lib.rows import duplicate_row
from lib.clean import clean_names, standardize_desc_cols
from lib.standardize import standardize_from_lookup_table
from lib.clean import clean_dates
from lib.uid import gen_uid
import pandas as pd
import io
import re
import datetime


def realign():
    with open(
        bolo.data("raw/baton_rouge_fpcsb/baton_rouge_fpcsb_logs_1992-2012.csv"),
        "r",
        encoding="latin-1",
    ) as f:
        # remove extraneous column names
        s = re.sub(
            r"(Date of Hearing|Hearing (Scheduled|Date)).?,(Docket|Case) #.+",
            "",
            f.read(),
        )
        s = re.sub(r",\s+$", ",", s)
        # add original column names
        s = "hearing_date,docket_no,name,action,resolution,spill\n" + s
        df = pd.read_csv(io.StringIO(s))

    # split row with 2 docket nos
    idx = df[df.docket_no == "12/6/07"].index[0]
    df = duplicate_row(df, idx)
    df.loc[idx, "docket_no"] = "12-06"
    df.loc[idx + 1, "docket_no"] = "12-07"
    df.loc[idx, "name"] = "Milton J. Cutrer"
    df.loc[idx + 1, "name"] = "Wilton R. Cutrer"

    # split rows with newline character
    i = 1
    for idx in df[df.name.fillna("").str.contains("\n")].index:
        df = duplicate_row(df, idx)
        df.loc[idx + i - 1, "docket_no"] = re.sub(
            r"^(\d+-\d+).+", r"\1", df.loc[idx + i - 1, "docket_no"].strip()
        )
        df.loc[idx + i, "docket_no"] = re.sub(
            r".+(\d+-\d+)$", r"\1", df.loc[idx + i, "docket_no"].strip()
        )
        df.loc[idx + i - 1, "name"] = re.sub(
            r"^(.+)\n.+", r"\1", df.loc[idx + i - 1, "name"].strip()
        )
        df.loc[idx + i, "name"] = re.sub(
            r".+\n(.+)$", r"\1", df.loc[idx + i, "name"].strip()
        )
        parts = df.loc[idx + i - 1, "action"].split("\n")
        if len(parts) == 2:
            [upper_action, lower_action] = parts
        else:
            m = re.match(r"^(\d+[^\d]+)\s*(\d+.+)$", df.loc[idx + i - 1, "action"])
            if m is not None:
                upper_action = m.group(1)
                lower_action = m.group(2)
            else:
                upper_action = df.loc[idx + i - 1, "action"]
                lower_action = df.loc[idx + i - 1, "action"]
        df.loc[idx + i - 1, "action"] = upper_action.strip()
        df.loc[idx + i, "action"] = lower_action.strip()

        i += 1

    df = df.dropna(subset=["name"]).reset_index(drop=True)
    df = df.drop(columns="spill")

    return df


def clean_docket_no(df):
    day_mo_pat = re.compile(r"^(\d+)-([A-Za-z]{3})$")
    ocr_err_pat1 = re.compile(r"(\d+) \d+-(\d+)")
    ocr_err_pat2 = re.compile(r"^(\d+)[^\d-]+(\d+)$")

    def replace(v):
        m = day_mo_pat.match(v)
        if m is not None:
            return "-".join(
                [
                    str(datetime.datetime.strptime(m.group(2), "%b").month).zfill(2),
                    m.group(1).zfill(2),
                ]
            )
        m = ocr_err_pat1.match(v)
        if m is not None:
            return "-".join([m.group(1), m.group(2)])
        m = ocr_err_pat2.match(v)
        if m is not None:
            return "-".join([m.group(1), m.group(2)])
        return v

    df.loc[:, "docket_no"] = (
        df.docket_no.fillna("")
        .str.strip()
        .str.replace("11-10 12-1", "11-10")
        .str.replace("0-01 10-03", "10-03")
        .str.replace("08-05 \n8-11", "08-11")
        .str.replace("11 LHE", "11-11")
        .str.replace("'", "")
        .str.replace(r"^702$", "07-02")
        .map(replace)
    )

    return df


def add_missing_year_to_hearing_date(df):
    def construct_date(year):
        def process(s):
            [day, month] = s.split("-")
            month = str(datetime.datetime.strptime(month, "%b").month)
            return "%s-%s-%s" % (year, month, day)

        return process

    df.loc[:, "appeal_hearing_date"] = (
        df.hearing_date.fillna("")
        .str.strip()
        .str.replace(r"\.", "")
        .str.replace(r"^(\d+)-(\d+)-(\d+)$", r"\1/\2/\3")
    )

    bool_idx = df.appeal_hearing_date.str.match(r"^\d+-[A-Za-z]{3}$")

    row_idx = bool_idx & (df.index <= 18)
    df.loc[row_idx, "appeal_hearing_date"] = df.loc[row_idx, "appeal_hearing_date"].map(
        construct_date("2006")
    )

    row_idx = bool_idx & (df.index > 18) & (df.index <= 26)
    df.loc[row_idx, "appeal_hearing_date"] = df.loc[row_idx, "appeal_hearing_date"].map(
        construct_date("2007")
    )

    row_idx = bool_idx & (df.docket_no == "01-06")
    df.loc[row_idx, "appeal_hearing_date"] = df.loc[row_idx, "appeal_hearing_date"].map(
        construct_date("2001")
    )

    row_idx = bool_idx & (df.docket_no == "08-12")
    df.loc[row_idx, "appeal_hearing_date"] = df.loc[row_idx, "appeal_hearing_date"].map(
        construct_date("2009")
    )

    row_idx = bool_idx & ((df.docket_no == "99-24") | (df.docket_no == "99-25"))
    df.loc[row_idx, "appeal_hearing_date"] = df.loc[row_idx, "appeal_hearing_date"].map(
        construct_date("1999")
    )

    df.loc[df.appeal_hearing_date == "Jan", "appeal_hearing_date"] = "2005-01"
    df.loc[df.appeal_hearing_date == "August", "appeal_hearing_date"] = "2004-08"
    df.loc[df.appeal_hearing_date == "Dec", "appeal_hearing_date"] = "2003-12"
    df.loc[df.appeal_hearing_date == "June", "appeal_hearing_date"] = "2007-06"

    return df.drop(columns="hearing_date")


def split_row_by_appeal_hearing_date(df):
    i = 0
    for idx in df[df.appeal_hearing_date.str.match(r".+(\&|\n).+")].index:
        try:
            [upper_date, lower_date] = re.split(
                r"\s*(?:\&|\n)\s*", df.loc[idx + i, "appeal_hearing_date"]
            )
        except ValueError:
            raise ValueError(df.loc[idx + i, "appeal_hearing_date"])
        if re.match(r"^\d+\/\d+$", upper_date):
            upper_date = "%s/%s" % (upper_date, lower_date.strip()[-2:])
        df = duplicate_row(df, idx)
        df.loc[idx + i, "appeal_hearing_date"] = upper_date
        df.loc[idx + i + 1, "appeal_hearing_date"] = lower_date
        i += 1
    return df


def standardize_hearing_date(df):
    date_pat1 = re.compile(r"^\d+\/\d+\/\d+$")
    date_pat2 = re.compile(r"^[A-Z][a-z]+, \d{4}$")

    def replace(v):
        m = date_pat1.match(v)
        if m is not None:
            [month, day, year] = v.split("/")
            if len(year) == 2:
                if int(year) < 30:
                    year = "20" + year
                else:
                    year = "19" + year
            return "-".join([year, month, day])
        m = date_pat2.match(v)
        if m is not None:
            dt = datetime.datetime.strptime(v, "%B, %Y")
            year = str(dt.year)
            month = str(dt.month)
            return "-".join([year, month])
        return v

    df.loc[:, "appeal_hearing_date"] = (
        df.appeal_hearing_date.str.replace(r"\/\/", "/")
        .str.replace("5/4/2000 9-21-2000", "2000-09-21")
        .str.replace("8-27-92 626-92", "1992-08-27")
        .map(replace)
        .str.replace(r"^[^\d].*", "")
    )

    dates = df.appeal_hearing_date.str.split("-", expand=True)
    dates.columns = [
        "appeal_hearing_year",
        "appeal_hearing_month",
        "appeal_hearing_day",
    ]
    dates.loc[:, "appeal_hearing_day"] = dates.appeal_hearing_day.fillna("").map(
        lambda x: x if not x.startswith("0") else x[1:]
    )
    dates.loc[:, "appeal_hearing_month"] = dates.appeal_hearing_month.fillna("").map(
        lambda x: x if not x.startswith("0") else x[1:]
    )
    df = pd.concat([df, dates], axis=1)
    df = df.drop(columns=["appeal_hearing_date"])

    return df


def extract_counsel(df):
    names = (
        df.name.str.strip()
        .str.replace("Tat Chi-Lam", "Tat-Chi Lam")
        .str.replace("Ashton-Dewey", "Ashton Dewey")
        .str.replace("Brian S. Ramey Floyd", "Brian S. Ramey - Floyd")
        .str.replace(r"\s*-\s*(\w+)$", r" - \1")
        .str.replace(r" \((.+)\)$", r" - \1")
        .str.replace(r"(\w+) fire$", r"\1 - fd")
        .str.split(r" - ", n=1, expand=True)
    )
    df.loc[:, "counsel"] = (
        names.loc[:, 1]
        .str.lower()
        .fillna("")
        .str.strip()
        .str.replace(r"\.", "")
        .str.replace(r"\b(dec|aug|nov|july|june|sept)\b", "")
        .str.replace(r"no representation", "none")
        .str.replace(r"\b(fd|fire d)\b", "fire dept")
        .str.replace(r"\b(paperwork|pending)\b", "")
    )
    df.loc[:, "appellant"] = names.loc[:, 0].fillna("")
    df = df.drop(columns=["name"])
    return df


def extract_appellant_rank(df):
    rank_name_df = pd.DataFrame.from_records(
        df.appellant.str.replace(r"^(Corporal|Cpl|Ofc|Sgt|Lt|Col)\.? (.+)", r"\1@@\2")
        .str.split("@@")
        .map(lambda x: x if len(x) == 2 else ("", x[0]))
        .tolist()
    )
    rank_m = {
        "cpl": "corporal",
        "ofc": "officer",
        "sgt": "sergeant",
        "lt": "lieutenant",
        "col": "colonel",
    }
    df.loc[:, "rank_desc"] = (
        rank_name_df.loc[:, 0].str.lower().map(lambda x: rank_m.get(x, x))
    )
    df.loc[:, "appellant"] = (
        rank_name_df.loc[:, 1]
        .str.strip()
        .fillna("")
        .str.lower()
        .str.replace(r"\.", "")
        .str.replace(r",\s+(jr|sr|i|ii|iii|iv|v)\b", r" \1")
        .str.replace(r"^\d", "")
        .str.strip()
    )
    return df


def split_rows_with_multiple_appellants(df):
    i = 0
    for idx in df[
        df.appellant.str.contains(",") | df.appellant.str.contains("&")
    ].index:
        s = df.loc[idx + i, "appellant"]
        parts = re.split(r"\s*(?:,|&)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "appellant"] = name
        i += len(parts) - 1
    return df


def drop_invalid_rows(df):
    df = df.drop(index=df[df.appellant == "brpd et al"].index)
    df = df.drop(index=df[df.docket_no == ""].index)
    return df.reset_index(drop=True)


def split_appellant_names(df):
    name_pat1 = re.compile(r"^([\w-]+(?: \w)?) ([\w']+(?: (?:jr|sr|i|ii|iii|iv|v))?)$")
    name_pat2 = re.compile(r"^(.+) (\w+)$")

    def replace(s):
        m = name_pat1.match(s)
        if m is not None:
            return "@@".join([m.group(1), m.group(2)])
        m = name_pat2.match(s)
        if m is not None:
            return "@@".join([m.group(1), m.group(2)])
        return "@@" + s

    names = df.appellant.map(replace).str.split("@@", expand=True)
    df.loc[:, "last_name"] = names.loc[:, 1].fillna("")
    names = names.loc[:, 0].fillna("").str.split(" ", expand=True)
    df.loc[:, "first_name"] = names.loc[:, 0].fillna("")
    df.loc[:, "middle_name"] = (
        names.loc[:, 1].fillna("").map(lambda s: "" if len(s) < 2 else s)
    )
    df.loc[:, "middle_name"] = names.loc[:, 1].fillna("").map(lambda s: s[:1])

    df = df.drop(columns="appellant")

    return df


def extract_appeal_disposition_date(df):
    appeal_disposition_date_lookup_table = [
        ["9/21/2000", "Appeal Withdrawn 9/21"],
        ["12/19/2000", "Replaced with Letter of Caution 12/19"],
        ["10/13/2011", "Settled w/ Chief 10-13"],
        ["2/21/1999", "Appeal Dropped 2/21"],
        ["9/30/1999", "Appeal Dropped 9/30"],
        ["12/18/2009", "Judge Trudy White Reinstated Termination on 12-18"],
        ["11/01/2009", "Leamer resigned 11-01", "Pate resigned 11-01"],
        ["6/16/2003", "Appeal Dropped 6/16"],
        ["3/2/2009", "Dropped 1-Day Suspension. Board upheld Termination 3-2"],
        ["12/18/2009", "Judge Trudy White Reinstated Termination on 12-18"],
        ["12/15/2011", "Reduced by Board to 3 days (12-15-11) Reduced by Board to 30"],
        ["12/28/1999", "Appeal Withdrawn 12/28/99"],
        ["1/19/2012", "Overturned by Board (01-19-12)"],
        ["12/20/2012", "Overturned by Board (12-20-2012)"],
        ["3/13/04", "Employee Retired 3/13/04"],
        ["2/5/2000", "Appeal Withdrawn 2/5/2000"],
        ["11/22/2000", "Appeal Dropped 11/22/2000"],
        ["05/17/2012", "Board approved Continuance to May 17, 2012 per Ms. LaFleur"],
        ["07/19/2012", "Overturned by Board (7/19/2012)"],
        ["01/19/2012", "Overturned by Board (01-19-2012)"],
        ["04/06/2001", "Letter of Caution 4/6/01"],
        ["1/06/2000", "Appeal Withdrawn 1/6/2000"],
        ["1/13/2000", "Appeal Withdrawn 1/13/2000"],
    ]
    dates = df.resolution.str.extract(r"((.+)(\d+)(.+))")
    df.loc[:, "appeal_disposition_date"] = dates[0].str.replace(r" $", "", regex=True)
    df = standardize_from_lookup_table(
        df, "appeal_disposition_date", appeal_disposition_date_lookup_table
    )
    return df


def clean_appeal_disposition(df):
    df.loc[:, "appeal_disposition"] = (
        df.resolution.str.lower()
        .str.strip()
        .str.replace(r"[ \/]+$", "")
        .str.replace(r"\b(\d+)\-?\/?(\d+)\b", "", regex=True)
        .str.replace(r"(\w+)-(\w+)", " ", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r"cont'd", "continuance", regex=True)
        .str.replace(r"(.+) ?withdrew ?(.+)", "withdrawn", regex=True)
        .str.replace(r"\bbd\b", "board", regex=True)
        .str.replace("feb", "february", regex=False)
        .str.replace(r"(\d+)day", r"\1-day", regex=True)
        .str.replace(r"^(\w+) resigned$", "resigned", regex=True)
        .str.replace(r"w/(\w+)", r"with \1", regex=True)
        .str.replace("w/", "with", regex=False)
        .str.replace(r"(^\:|\/|\-|\.|\(|\)|\\|\,|^\?$)", "", regex=True)
        .str.replace("deciison", "decision", regex=False)
        .str.replace(r"\bch\b", "chief", regex=True)
        .str.replace(r"\bsusp\b", "suspension", regex=True)
        .str.replace(r"cancelled(\w+)", r"cancelled \1", regex=True)
        .str.replace(r"continued(\w+)", r"continued \1", regex=True)
        .str.replace(r"\brep\b", "reprimand", regex=True)
        .str.replace("mf&pcs", "municipal fire and police civil service", regex=False)
        .str.replace("recv'd", "received", regex=False)
        .str.replace("appelent", "appellant", regex=False)
        .str.replace("ltr", "letter", regex=False)
        .str.replace("lt", "lieutenant", regex=False)
        .str.replace(r"day(\w+)", r"day \1", regex=True)
        .str.replace("hrs", "hours", regex=False)
        .str.replace("prev", "previous", regex=False)
        .str.replace("indef", "indefinitely", regex=False)
        .str.replace(r"\bdist\b", "district", regex=True)
        .str.replace(r"\bct\b", "court", regex=True)
        .str.replace(r"dirks(\w+)", r"dirks \1", regex=True)
        .str.replace(r"\bdec\b", "december", regex=True)
        .str.replace(r"\bdept\b", "department", regex=True)
        .str.replace(r"continued(\w+)", r"continued \1", regex=True)
        .str.replace(r"  +", " ", regex=True)
    )
    return df.drop(columns="resolution")


def remove_invalid_rows(df):
    df = df[df.counsel != "fire dept"]
    df = df[df.last_name != ""]
    return df.reset_index(drop=True)


def assign_counsel_for_empty_rows(df):
    counsel_dict = (
        df.loc[df.counsel != "", ["docket_no", "counsel"]]
        .drop_duplicates(subset=["docket_no"])
        .set_index("docket_no")
        .counsel.to_dict()
    )
    df.loc[df.counsel == "", "counsel"] = df.loc[df.counsel == "", "docket_no"].map(
        lambda x: counsel_dict.get(x, "")
    )
    return df


def clean_action(df):
    lookup_table = [
        ["1/2-day suspension"],
        ["1-day suspension", "1-day"],
        ["2-day suspension"],
        ["2 1/2-day suspension"],
        ["3-day suspension"],
        ["4-day suspension"],
        ["5-day suspension"],
        ["6-day suspension"],
        ["10-day suspension"],
        ["14-day suspension"],
        ["15-day suspension"],
        ["20-day suspension"],
        ["21-day suspension"],
        ["28-day suspension"],
        ["30-day suspension"],
        ["45-day suspension"],
        ["60-day suspension"],
        ["65-day suspension"],
        ["69-day suspension"],
        ["12-hour suspension"],
        ["80-hour suspension"],
        ["1/2-day pay"],
        ["30-day vehicle use suspension"],
        ["termination", "termination/retirement", "termination (fd)"],
        ["transfer", "transfers", "involuntary transfer"],
        ["letter of reprimand", "ltr of rep"],
        ["payroll check docked"],
        ["42-day suspension"],
        ["exam rejection"],
        ["re: lt. exam"],
        ["drug testing procedures"],
        ["15 dy unitloss"],
        ["demotion"],
        ["dismissal"],
        ["paid adm. leave"],
        ["termination wired"],
        ["suspension - extra"],
        ["adm. leave without pay"],
        ["request for adm. hearing (promotion to sgt.)"],
        ["placed on restrictive duty"],
        ["vehicle use suspension", "veh use suspension", "vehicle suspension"],
        ["appeal denied by board"],
        ["placed on leave w/o pay"],
        ["refusal to allow him to return to work"],
        ["appealing transfer"],
        ["rehearing of appeal"],
        ["letter of caution"],
        ["reinstatement"],
    ]
    df.loc[:, "action"] = (
        df.action.str.strip()
        .fillna("")
        .str.lower()
        .str.replace(r"[ \:]+$", "")
        .str.replace("10 20 day suspension", "10-day suspension")
        .str.replace(r"(\d)[\s-]+days?", r"\1-day")
        .str.replace("repimand", "reprimand")
        .str.replace(r"(\d)[\s-]+(?:hrs\.|hours?)", r"\1-hour")
        .str.replace(r"(susp\.|suspensiod|sugpension|suspension)", "suspension")
    )
    df = standardize_from_lookup_table(df, "action", lookup_table)
    return df


def condense_rows_with_same_docket_no(df):
    for idx1, row1 in df[df.appeal_disposition.isna()].iterrows():
        for idx2, row2 in df[df.docket_no == row1.docket_no].iterrows():
            if (
                idx1 == idx2
                or row1.action != row2.action
                or row1.counsel != row2.counsel
                or row1.last_name != row2.last_name
                or row1.middle_name != row2.middle_name
                or row1.first_name != row2.first_name
                or row1.rank_desc != row2.rank_desc
                or (
                    row1.appeal_hearing_year != ""
                    and row2.appeal_hearing_year != row1.appeal_hearing_year
                )
            ):
                continue
            if pd.notnull(row2.appeal_disposition):
                df.loc[idx1, "appeal_disposition"] = row2.appeal_disposition
            if row2.appeal_hearing_year != "":
                df.loc[idx1, "appeal_hearing_year"] = row2.appeal_hearing_year
                df.loc[idx1, "appeal_hearing_month"] = row2.appeal_hearing_month
                df.loc[idx1, "appeal_hearing_day"] = row2.appeal_hearing_day
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "Baton Rouge PD"
    df.loc[:, "data_production_year"] = 2012
    return df


def clean():
    df = realign()
    df = (
        df.pipe(clean_docket_no)
        .pipe(add_missing_year_to_hearing_date)
        .pipe(split_row_by_appeal_hearing_date)
        .pipe(standardize_hearing_date)
        .pipe(extract_counsel)
        .pipe(extract_appellant_rank)
        .pipe(split_rows_with_multiple_appellants)
        .pipe(drop_invalid_rows)
        .pipe(split_appellant_names)
        .pipe(assign_agency)
        .pipe(clean_names, ["first_name", "last_name", "counsel"])
        .pipe(extract_appeal_disposition_date)
        .pipe(clean_appeal_disposition)
        .pipe(remove_invalid_rows)
        .pipe(assign_counsel_for_empty_rows)
        .pipe(clean_action)
        .pipe(condense_rows_with_same_docket_no)
        .pipe(standardize_desc_cols, ["appeal_disposition"])
        .pipe(clean_dates, ["appeal_disposition_date"])
        .rename(columns={"action": "action_appealed"})
        .pipe(
            gen_uid,
            ["agency", "first_name", "last_name", "middle_name"],
        )
        .pipe(gen_uid, ["uid", "docket_no"], "appeal_uid")
        .pipe(
            gen_uid,
            [
                "appeal_uid",
                "appeal_hearing_year",
                "appeal_hearing_month",
                "appeal_hearing_day",
                "appeal_disposition",
            ],
            "appeal_disposition_uid",
        )
    )
    return df.drop_duplicates().reset_index(drop=True)


if __name__ == "__main__":
    df = clean()

    df.to_csv(bolo.data("clean/lprr_baton_rouge_fpcsb_1992_2012.csv"), index=False)

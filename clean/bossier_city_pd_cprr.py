import deba
import pandas as pd
from lib.columns import clean_column_names
from lib.clean import clean_dates
from lib.rows import duplicate_row
import re
from lib.uid import gen_uid


def extract_receive_date(df):
    dates = df.atic_n_date_iad_numbes.str.extract(r"(\d+\/\d+\/\d+)")
    df.loc[:, "receive_date"] = dates[0]
    return df


def clean_tracking_id(df):
    df.loc[:, "tracking_id"] = df.atic_n_date_iad_numbes.str.replace(
        r"(.+) (20-.+) ", r"\2", regex=True
    ).str.replace("1A", "IA", regex=False)
    return df.drop(columns="atic_n_date_iad_numbes")


def clean_complaint_type(df):
    df.loc[:, "complaint_type"] = (
        df.chassification.str.lower()
        .str.strip()
        .str.replace(r"dep[ea]rtmenta?[lds]\!?", "departmental", regex=True)
        .str.replace(r"informa[tdl]c?", "informal", regex=True)
    )
    return df.drop(columns="chassification")


def clean_allegations(df):
    df.loc[:, "allegation"] = (
        df.type_complaint.str.lower()
        .str.strip()
        .str.replace(r"^coda", "code", regex=True)
        .str.replace(r"^derefiction", "dereliction", regex=True)
        .str.replace("pursuit policy violation", "vehicle pursuit", regex=False)
        .str.replace(
            r"^st[ea]ndard[as] of c[ao]nduct$", "standards of conduct", regex=True
        )
        .str.replace(r"hand[ae]uft?ing", "handcuffing", regex=True)
        .str.replace(r"\, ", "/", regex=True)
        .str.replace(
            "standards of conduct dereliction",
            "standards of conduct/dereliction of duty",
            regex=False,
        )
        .str.replace("discrimination/haras ment", "discrimination and harassment")
    )
    return df.drop(columns="type_complaint")


def split_rows_with_multiple_allegations(df):
    i = 0
    for idx in df[df.allegation.str.contains(r"/")].index:
        s = df.loc[idx + i, "allegation"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "allegation"] = name
        i += len(parts) - 1
    return df


def extract_and_clean_disposition(df):
    extracted_dispositions = (
        df.action_taken.str.lower()
        .str.strip()
        .str.extract(r"(unfounded|not sustained|exonderated)")
    )
    df.loc[:, "dispositions_extracted"] = extracted_dispositions[0].str.replace(
        "exonderated", "exonerated"
    )
    df.loc[:, "disposition"] = (
        df.disposition.str.lower()
        .str.strip()
        .str.replace(r"sus[ti]m?i?a?l?ned", "sustained", regex=True)
        .str.replace(r"(\w+)- ?sustained", "not sustained", regex=True)
        .str.replace(r"(none|n\/a)", "", regex=True)
        .str.replace(r"unf ?oun[dt][ea]d", "unfounded", regex=True)
        .str.replace(r"syst ?em fai?tur[ea]", "system failure", regex=True)
        .str.replace(r"( ?miscondue?l?c?t?| ?x?2x?)", "", regex=True)
        .str.replace(r"\,$", "", regex=True)
        .str.replace(r"(\/|\,\, )", "|", regex=True)
    )
    df.loc[:, "disposition"] = df.disposition.fillna("").str.cat(
        df.dispositions_extracted.fillna(""), sep=""
    )
    return df.drop(columns="dispositions_extracted")


def clean_actions(df):
    df.loc[:, "action"] = (
        df.action_taken.str.lower()
        .str.strip()
        .str.replace(r"(unfounded|not sustained|[hn]one|exonderated)", "", regex=True)
        .str.replace(r"declined .+", "", regex=True)
        .str.replace(r"decin?[nm]ed", "declined", regex=True)
        .str.replace("termination", "terminated", regex=False)
        .str.replace("worlsy", "worley", regex=False)
        .str.replace("los", "loss of seniority", regex=False)
        .str.replace("lop", "loss of pay", regex=False)
        .str.replace(
            "day suspension loss of senioritys of senority and pay",
            "5 day suspension|loss of senority|loss of pay",
        )
        .str.replace(
            "5 day suspension loss of senioritys x2 of seniority",
            "5 day suspension|loss of seniority",
            regex=False,
        )
        .str.replace(r"\, ", "|", regex=True)
    )
    return df.drop(columns="action_taken")


def clean_investigating_supervisor(df):
    df.loc[:, "investigating_supervisor"] = (
        df.investigatodate.str.lower()
        .str.strip()
        .fillna("")
        .str.replace("n/a", "", regex=False)
        .str.replace("meck", "mack", regex=False)
    )
    return df.drop(columns="investigatodate")


def combine_officer_and_other_officer_columns(df):
    df.loc[:, "officer"] = (
        df.officer.str.lower()
        .str.strip()
        .fillna("")
        .str.cat(df.other_officers.str.lower().str.strip().fillna(""), sep="/")
        .str.replace(
            "jeter, fanning. achanfer, gallon, estess, willams",
            "burr/jeter, fanning/achanfer, gallon/estess, willams",
            regex=False,
        )
        .str.replace("; ", "/", regex=False)
        .str.replace(r"delectives/officer \$", "detectives", regex=True)
        .str.replace(r"( \$|0|unknawn)", "", regex=True)
        .str.replace(r"\.", ",", regex=True)
        .str.replace(r"^jahnson", "johnson", regex=True)
        .str.replace(r"(\w+) /(\w+)", r"\1/\2", regex=True)
        .str.replace(r"\/$", "", regex=True)
        .str.replace("benjamin, deaveon", "benjamin/deaveon", regex=False)
    )
    return df


def split_rows_with_multiple_officers_and_split_names(df):
    i = 0
    for idx in df[df.officer.fillna("").str.contains(r"/")].index:
        s = df.loc[idx + i, "officer"]
        parts = re.split(r"\s*(?:\/)\s*", s)
        df = duplicate_row(df, idx + i, len(parts))
        for j, name in enumerate(parts):
            df.loc[idx + i + j, "officer"] = name
        i += len(parts) - 1

    names = df.officer.str.lower().str.strip().str.extract(r"(\w+),? ?(\w+)?")
    df.loc[:, "last_name"] = names[0].fillna("")
    df.loc[:, "first_name"] = names[1].fillna("")
    return df.drop(columns=["officer", "other_officers"])


def clean_allegation_desc(df):
    df.loc[:, "allegation_desc"] = (
        df.synopsis.str.lower()
        .str.strip()
        .str.replace("bcpd", "bossier city police department", regex=False)
        .str.replace(r"all[ae]g[ae][sn]", "alleges", regex=True)
        .str.replace("sicla", "stole", regex=False)
        .str.replace(r"[oa]ffic[ae]rs\b", "officers", regex=True)
        .str.replace(r"[oa]ffic[ae]r\b", "officer", regex=True)
        .str.replace("juvenila", "juvenile", regex=False)
        .str.replace(r"sg[tl].", "sergeant", regex=True)
        .str.replace(r"cpl\.", "corporal", regex=True)
        .str.replace("palicy", "policy", regex=False)
        .str.replace("det.", "detective", regex=False)
        .str.replace("mot", "not", regex=False)
        .str.replace("apices", "spices", regex=False)
        .str.replace("spois", "spots", regex=False)
        .str.replace("tickeled", "ticketed", regex=False)
        .str.replace("trattic", "traffic", regex=False)
        .str.replace(r"purs?a?uc?i?ti?", "pursuit", regex=False)
        .str.replace("invalving", "involving", regex=False)
        .str.replace("rudaness", "rudeness", regex=False)
        .str.replace(
            "accused him of was rude and threaten jail.",
            "accused him of drinking, was rude and threaten jail.",
            regex=False,
        )
        .str.replace("rekused", "refused", regex=False)
        .str.replace("disrespectivi", "disrespectful", regex=False)
        .str.replace("lo", "to", regex=False)
        .str.replace("binandon", "brandon", regex=False)
        .str.replace("taka", "take", regex=False)
        .str.replace("inapproapiala facabock", "inappropriate faceboook", regex=False)
        .str.replace("calis", "calls", regex=False)
        .str.replace("conduci", "conduct", regex=False)
        .str.replace("avicion", "eviction", regex=False)
        .str.replace("injurtes", "injuries", regex=False)
        .str.replace("lis", "his", regex=False)
        .str.replace("off-cury", "off-duty", regex=False)
        .str.replace("quirty", "guilty", regex=False)
        .str.replace(r"[ln]is", "his", regex=True)
        .str.replace("recaive", "receive", regex=False)
        .str.replace("jaid", "jail", regex=False)
        .str.replace("shat", "shut", regex=False)
        .str.replace("spesding", "spreading", regex=False)
        .str.replace("sicohol", "alcohol", regex=False)
        .str.replace(r"fa[il]ted", "failed", regex=True)
        .str.replace("unprofessionel", "unprofessional", regex=False)
        .str.replace("merked", "marked", regex=False)
        .str.replace(r"\ble\b", "in", regex=True)
        .str.replace(r"\bpursucti\b", "pursuit", regex=True)
        .str.replace(r"\bpart\b", "park", regex=True)
        .str.replace(r"\btrafte\b", "traffic", regex=True)
        .str.replace(r"\bdesth\b", "death", regex=True)
        .str.replace(r"\btocation\b", "location", regex=True)
        .str.replace(r"\blicksts\b", "tickets", regex=True)
    )
    return df.drop(columns="synopsis")


def assign_dispositions(df):
    df.loc[
        (df.tracking_id == "20-IA-53") & (df.last_name == "morton"), "disposition"
    ] = "sustained"
    df.loc[
        (df.tracking_id == "20-IA-53") & (df.last_name == "levy"), "disposition"
    ] = "unfounded"
    df.loc[
        (df.tracking_id == "20-IA-53") & (df.last_name == "holmes"), "disposition"
    ] = "exonerated"
    df.loc[
        (df.tracking_id == "20-IA-58") & (df.last_name == "sproles"), "disposition"
    ] = ""
    df.loc[
        (df.tracking_id == "20-IA-58") & (df.last_name == "bridges"), "disposition"
    ] = ""
    df.loc[
        (df.tracking_id == "20-IA-38") & (df.last_name == "benjamin"), "disposition"
    ] = "sustained"
    df.loc[
        (df.tracking_id == "20-IA-38") & (df.last_name == "deaveon"), "disposition"
    ] = "unfounded"
    return df


def assign_action(df):
    df.loc[(df.tracking_id == "20-IA-53"), "action"] = ""
    df.loc[
        (df.tracking_id == "20-IA-38") & (df.last_name == "benjamin"), "action"
    ] = "5 day suspension|loss of senority|loss of pay"
    df.loc[(df.tracking_id == "20-IA-38") & (df.last_name == "deaveon"), "action"] = ""
    return df


def assign_agency(df):
    df.loc[:, "agency"] = "bossier-city-pd"
    return df


def create_tracking_id_og_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/bossier_city_pd/bossier_city_pd_cprr_2020.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "assigned": "investigation_start_date",
                "date_returned": "investigation_complete_date",
            }
        )
        .pipe(extract_receive_date)
        .pipe(clean_tracking_id)
        .pipe(clean_complaint_type)
        .pipe(clean_allegations)
        .pipe(split_rows_with_multiple_allegations)
        .pipe(combine_officer_and_other_officer_columns)
        .pipe(split_rows_with_multiple_officers_and_split_names)
        .pipe(extract_and_clean_disposition)
        .pipe(clean_actions)
        .pipe(clean_investigating_supervisor)
        .pipe(clean_allegation_desc)
        .pipe(assign_dispositions)
        .pipe(assign_action)
        .pipe(assign_agency)
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            ["uid", "allegation", "disposition", "tracking_id", "action"],
            "allegation_uid",
        )
        .pipe(
            clean_dates,
            ["receive_date", "investigation_complete_date", "investigation_start_date"],
        )
        .pipe(create_tracking_id_og_col)
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_bossier_city_pd_2020.csv"), index=False)

import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, strip_leading_comma, standardize_desc_cols
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
    df.loc[:, "complainant_type"] = (
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


def extract_receive_date(df):
    dates = df.date.str.extract(r"(\w{1,2}\/(\w{1,2}\/\w{4}))")

    df.loc[:, "receive_date"] = dates[0]
    return df.drop(columns=["date"])

def extract_tracking_id(df):
    id = df.iad_number.str.replace(r"(1|I)A-", "", regex=True).str.extract(r"(\w{1,2}-\w{1,3})")
    
    df.loc[:, "tracking_id_og"] = id[0]
    return df.drop(columns=["iad_number"])

def split_rows_with_multiple_officers(df):
    df.loc[:, "officer"] = (df.officer
                            .str.replace(r":", ";", regex=False)
                            .str.replace(r"^Stewart Jamie Easterling, J$", "", regex=True)
                            .str.replace(r"^McGee Turner. Wheatty Provost$", "", regex=True)
                            .str.replace(r"^Kutz Harvey Roberts$", "", regex=True)
                            .str.replace(r"^(Unknown Officers|unk|Un?known|Communications|Patrol\, Detectives|UNKNOWN)$","", regex=True)
                            .str.replace(r"^(\w+)\. (\w+)$", r"\1, \2", regex=True)
                            .str.replace(r"^Warren, Thomerson Sanford$", "", regex=True)
                            .str.replace(r"^(\w{1})\.? (\w+)$", r"\2, \1")
                            .str.replace(r"^(\w+) (\w{1})\.$", r"\1, \2", regex=True)
                            .str.replace(r"(\w{1})\.$", r"\1", regex=True)
                            .str.replace(r"(\w+)\. (\w{1})$", r"\1, \2", regex=True)
                            .str.replace(r"(.+)?Sgt(.+)?", r"Sgt \1 \2", regex=True)
                            .str.replace(r"(\w+)\.(\w{1})", r"\1, \2", regex=True)
                            
    )

    df.loc[:, "other_officers"] = (df.other_officers
                                   .str.replace(r"(\w+) (\w{1}) \b", r"\1 \2;", regex=True)
                                   .str.replace(r"^(\w+)\, (\w+)\, (\w+)\, (\w+)\, (\w+)\, (\w+)$", r"\1; \2; \3; \4; \5; \6", regex=True)
                                   .str.replace(r"^(\w{1})\. (\w+)\, (\w{1})\. (\w+)$", r"\1. \2; \3. \4", regex=True)
                                   .str.replace(r"^(\w+)\, (\w{1})\.?\, (\w+)\, (\w{1})$", r"\1 \2; \3 \4", regex=True)
                                   .str.replace(r"(\w{1})\. (\w+)$", r"\2, \1", regex=True)
                                   .str.replace(r"(\/|:|\.)", ";", regex=True)
                                   .str.replace(r"^(bcpd|jail personal)", "", regex=True)
                                   .str.replace(r"^Res ofcs Ball and Boing", "Ball; Boing", regex=True)
                                   .str.replace(r"^Sproies E Freeman K WhenDey$", "", regex=True)
                                   .str.replace(r"^Wood.S nan,Merriott$", "", regex=True)
                                   .str.replace(r"^(\w+) (\w{1}) (\w+) (\w{1}) (\w+) (\w{1})$", r"\1, \2; \3, \4; \5, \6", regex=True)
                                   .str.replace(r"^(\w+)\, (\w{1}) (\w+)\, (\w{1}) (\w+) (\w{1})$", r"\1, \2; \3, \4; \5, \6", regex=True)
                                   .str.replace(r"(Unknown Others|SCIU|HUMPHREY, CHEATWOOD COBB|Unknown)", "", regex=True)
                                   .str.replace(r"^McGee, Faktor, Sproles$", "", regex=True)
                                   .str.replace(r"^(\w{1})\. (\w+)\, (\w{1})\.(\w+)\, (\w{1}) (\w+)", r"\2, \1; \4, \3; \6, \5", regex=True)
                                   .str.replace(r"^(wamp THANTS, PROOU o. Calla|wamp THANTS, PROOU Calla, o)$", "", regex=True)
                                   .str.replace(r"^(LIDDELL BRICE, BOOKER|S; DURR|ENGI|DSI)$", "", regex=True)
                                   .str.replace(r"^(PROVOST SWAN C|HAMM BLOUNT NELSON)$", "", regex=True)
                                   .str.replace(r"(.+)?Sgt(.+)?", r"Sgt \1, \2", regex=True)
                                   .str.replace(r"^Sgt Roberts, Mitchell, Rambo, , ; Johnson, M$", "", regex=True)
                                   .str.replace(r"^Hardesty, McDonald, Barclay$", "Hardesty; McDonald; Barclay", regex=True)
                                   .str.replace(r"^MarC, r$", "", regex=True)

                                  
    )
    df = (
        df.drop("officer", axis=1)
        .join(
            df["officer"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("officer"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    df = (
        df.drop("other_officers", axis=1)
        .join(
            df["other_officers"]
            .str.split(";", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("other_officers"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    dfa = df.copy()
    dfa = dfa.drop(columns=["other_officers"])

    dfb = df.copy()
    dfb = dfb.drop(columns=["officer"]).rename(columns={"other_officers": "officer"})

    concat_df = pd.concat([dfa, dfb])
    return concat_df


def split_names(df):
    df.loc[:, "officer"] = (df.officer
                            .str.replace(r"^(\w+)\, (\w+)\, (\w+)$", "", regex=True)
                            .str.replace(r"(CID|BCPD)", "", regex=True)
                            .str.replace(r"^ (\w+)", r"\1", regex=True)
                            .str.replace(r"^(Aguirre\, G Estees|McWhiney\, Jones Kelly|CamMike\, p|KerrDarren\, y)$", "", regex=True)
                            .str.replace(r"^(NELSON NUNNERY, N|SWAN WELLS, WOOD|HAMMERSLA|SOUTER, CULVER NELSON BRYANT)$", "", regex=True)
                            .str.replace(r"^(ENGL, HAUGEN|ProvosLiddell, t|Sepulvado Taylor, Payne)$", "", regex=True)
                            .str.replace(r"^SgGray, t$", "Sgt Gray", regex=True)
                            .str.replace(r"^(Culver, Fuller, Sepulvado, Cole, Yetman)$", "", regex=True)
                            .str.replace(r"^(Thompson, 2| \$)$", "", regex=True)
                            .str.replace(r"^(\w+)\,(\w{1})$", r"\1, \2", regex=True)
                            .str.replace(r"V\, BROWN", "BROWN, V", regex=True)
                            .str.replace(r"^(\w{1})$", "", regex=True)
                            .str.replace(r"^CRU$", "", regex=True)
    )
    names = df.officer.str.lower().str.strip().str.extract(r"(sgt|pco)? ?(\w+)\,? ?(.+)?$")

    df.loc[:, "rank_desc"] = names[0]
    df.loc[:, "last_name"] = names[1].fillna("")
    df.loc[:, "first_name"] = names[2].fillna("")
    return df.drop(columns=["officer"])


def clean_allegation10(df):
    df.loc[:, "allegation"] = (df.type_complaint
                               .str.lower()
                               .str.strip()
                               .str.replace(r"no pc\b", "professional conduct", regex=True)
                               .str.replace(r"^(code conduct code conduct|code conduct|code to conduct)$", "code of conduct", regex=True)
                               .str.replace(r"^(code conduct dereliction duty|dereliction duty code conduct)$", "code of conduct; dereliction of duty", regex=True)
                               .str.replace(r"rudenes s", "rudeness", regex=False)
                               .str.replace(r"violatioin", "violation", regex=False)
                               .str.replace(r"(derelictio n|dereli ction)$", "dereliction of duty", regex=True)
                               .str.replace(r"^(dereliction duty|derefiction duty|deriliction of duty|deleriction of duty)$", "dereliction of duty", regex=True)
                               .str.replace(r"haras sment", "harrassment", regex=False)
                               .str.replace(r"^rudeness rudeness$", "rudeness", regex=True)
                               .str.replace(r"rude\. unprofessional", "rudeness; unprofessional", regex=True)
                               .str.replace(r"^dereliction$", "dereliction of duty", regex=True)
                               .str.replace(r"know & comply", "know and comply", regex=False)
                               .str.replace(r"unprofession nal", "unprofessionnal", regex=False)
                               .str.replace(r"(derelict on|derelicton|derelection)$", "dereliction of duty", regex=True)
                               .str.replace(r"^veh pursuit$", "vehicle pursuit", regex=True)
                               .str.replace(r"rud eness$", "rudeness", regex=True)
                               .str.replace(r"^dereliction of duty towing policy$", "dereliction of duty; towing policy", regex=True)
                               .str.replace(r"rude\, illegal detention", "rude-illegal detention", regex=True)
                               .str.replace(r"(false arrest\/excessive force|false arrest\/exessive force|"
                                            r"excessive force\/ false arrest|false arrest\, excessive forc)", "false arrest-excessive force", regex=True)
                               .str.replace(r"^(excessive force\/dereliction of duty)$", "excessive force; dereliction of duty", regex=True)
                               .str.replace(r"(.+)\/(.+)", r"\1; \2", regex=True)
                               .str.replace(r"^dereliction;(.+)", r"dereliction of duty; \1", regex=True)
                               .str.replace(r"in-custody", "in custody", regex=False)
                               .str.replace(r"rude\, professional conduct", "rudeness;professional conduct", regex=True)
                               .str.replace(r"(.+);\s+(.+)", r"\1;\2", regex=True)
                               .str.replace(r"illegal stop and search\, professional conduct for ticket", 
                                            "illegal stop and search; professional conduct for ticket", regex=True)
                                .str.replace(r"standard of conduct", "standards of conduct", regex=True)
                                .str.replace(r"unprofessionnal", "unprofessional", regex=False)
                                .str.replace(r"excessive force\, mve", "excessive force;mve violation", regex=True)
                                .str.replace(r"rude;", "rudeness;", regex=False)
                                .str.replace(r"^rude$", "rudeness", regex=True)
                                .str.replace(r"false arrest\, rudeness", "false arrest-rudeness", regex=True)
                                .str.replace(r"incustody", "in custody", regex=False)
                                .str.replace(r"^mve policy$", "mve violation", regex=True)
                                
    )
    return df[~(df.allegation.fillna("") == "")].drop(columns=["type_complaint"])


def split_rows_with_multiple_allegations_10(df):
    df = (
        df.drop("allegation", axis=1)
        .join(
            df["allegation"]
            .str.split("-", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("allegation"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


def extract_disposition(df):
    dis = df.disposition.str.lower().str.strip().str.extract(r"^(not sustained|sustained|unfounded|exonerated)$")

    df.loc[:, "disposition"] = dis[0]
    return df


def extract_action(df):
    action = (df.action_taken
              .str.lower()
              .str.strip()
              .str.extract(r"^(termination|letter of reprimand|"
                           r"verbal counsel|resigned|1 day without pay"
                           r"|lor|ofc resigned|officer resigned"
                           r"|demotion|officer retired|1 day suspension|counseled|resigned"
                           r"3 day suspension|(\w{1,2}) days? suspension)$")) 
    df.loc[:, "action"] = (action[0]
                           .str.replace(r"^lor$", "letter of reprimand", regex=True)
                           .str.replace(r"^(\w{1,2}) days? suspension", r"\1-day suspension", regex=True)
                           .str.replace(r"^(\w+) resigned", r"resigned", regex=True)
                           .str.replace(r"(\w+) retired", "retired", regex=True)
    )
    return df.drop(columns=["action_taken"])


def clean_investigator_10(df):
    investigators = (df.investigator
                     .str.lower()
                     .str.strip()
                     .str.replace(r"^(na|none|declined investigation|declined|\/)$", "", regex=True)
                     .str.extract(r"^(sgt|lt|capt|chief)?\.? ?(?:(\w+\.?) )? ?(\w+)$")
    )
    df.loc[:, "investigator_rank_desc"] = (investigators[0]
                                           .str.replace(r"sgt", "sergeant", regex=False)
                                           .str.replace(r"lt", "lieutenant", regex=False)
                                           .str.replace(r"capt", "captain", regex=False)
    )
    df.loc[:, "investigator_first_name"] = investigators[1].str.replace(r"(^j[32m]|\.)", "", regex=True)
    df.loc[:, "investigator_last_name"] = investigators[2]
    return df.drop(columns=["investigator"])


def fix_dates(df):
    df.loc[:, "investigation_start_date"] = (df.investigation_start_date
                                             .str.replace(r"(\w{2})$", r"20\1", regex=True)
                                             .str.replace(r"(.+)\s+(.+)", "", regex=True)
                                             .str.replace(r"^00\/(.+)", "", regex=True)
    )
    df.loc[:, "investigation_complete_date"] = (df.investigation_complete_date
                                                .str.replace(r"(\w{2})$", r"20\1", regex=True)
                                                .str.replace(r"(.+)\s+(.+)", "", regex=True)
                                                .str.replace(r"^00\/(.+)", "", regex=True)
                                                .str.replace(r"^0$", "", regex=True)
    )
    return df 


def clean():
    df = (pd.read_csv(deba.data("raw/bossier_city_pd/bossier_city_pd_cprr_2010_2020.csv"))
          .pipe(clean_column_names)
          .drop(columns=["classification", "comp_name", "comp_phone"])
          .rename(columns={"synopsis": "allegation_desc", "date_assigned": "investigation_start_date", 
                           "date_returned": "investigation_complete_date"})
          .pipe(strip_leading_comma)
          .pipe(extract_receive_date)
          .pipe(extract_tracking_id)
          .pipe(split_rows_with_multiple_officers)
          .pipe(split_names)
          .pipe(clean_allegation10)
          .pipe(split_rows_with_multiple_allegations_10)
          .pipe(extract_disposition)
          .pipe(extract_action)
          .pipe(clean_investigator_10)
          .pipe(fix_dates)
          .pipe(clean_dates, ["receive_date", "investigation_start_date", "investigation_complete_date"])
          .pipe(standardize_desc_cols, ["allegation_desc"])
          .pipe(set_values,  {"agency": "bossier-city-pd"} )
          .pipe(gen_uid, ["tracking_id_og", "agency"], "tracking_id")
          .pipe(gen_uid, ["first_name", "last_name", "agency"])
          .pipe(gen_uid, ["allegation", "allegation_desc", "disposition", "uid", "receive_day", "investigation_complete_day"], "allegation_uid")
          .pipe(gen_uid, ["investigator_first_name", "investigator_last_name", "agency"], "investigator_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_bossier_city_pd_2010_2021.csv"), index=False)

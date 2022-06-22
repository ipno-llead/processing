import deba
import pandas as pd
from lib.clean import names_to_title_case
from lib.columns import set_values
from lib.uid import gen_uid


def stack_df(df):
    df = df[
        [
            "allegation",
            "allegation_1",
            "allegation_2",
            "allegation_3",
            "allegation_4",
            "allegation_5",
            "allegation_6",
            "allegation_7",
            "allegation_8",
            "allegation_9",
            "allegation_10",
            "allegation_11",
            "allegation_12",
            "allegation_13",
            "allegation_14",
            "allegation_15",
            "allegation_16",
            "allegation_17",
            "allegation_18",
            "allegation_19",
            "allegation_20",
            "allegation_21",
            "allegation_22",
            "allegation_23",
            "allegation_24",
            "allegation_25",
            "allegation_26",
            "allegation_27",
            "allegation_28",
            "allegation_29",
            "allegation_30",
            "allegation_31",
            "allegation_32",
            "allegation_33",
            "allegation_34",
            "allegation_35",
            "allegation_36",
            "allegation_37",
            "allegation_38",
        ]
    ].stack()

    df.name = "allegation"
    df = df.reset_index().drop(columns=["level_0", "level_1"])

    return df


def clean_allegation(df):
    df.loc[:, "allegation"] = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"^ +", "", regex=True)
        .str.replace(r"\n+?", " ", regex=True)
        .str.replace(r"received:? ", "", regex=True)
        .str.replace(r"\n+", "", regex=True)
        .str.replace(
            r"\ballegation:? nfim\b", "no formal investigation merited", regex=True
        )
        .str.replace(r"\(?nfim\)?", "", regex=True)
    )
    return df


def extract_tracking_id(df):
    tracking = df.allegation.str.replace(
        r"(complaint tracking number:?|control number:?)", "", regex=True
    ).str.extract(r"(201[456789]-\w{4}[\.-]\w)[:_]?")
    df.loc[:, "tracking_id"] = tracking[0]
    return df


def extract_receive_date(df):
    dates = df.allegation.str.replace(
        r"(\w{1,2})-(\w{1,2})-(\w{2,4})", r"\1/\2/\3", regex=True
    ).str.extract(r"(\w{1,2}\/\w{1,2}\/\w{2,4})")
    df.loc[:, "receive_date"] = dates[0]
    return df


def extract_disposition(df):
    dispositions = (
        df.allegation.str.lower()
        .str.strip()
        .str.replace(r"disposition:? (na|none)", "no disposition", regex=True)
        .str.extract(
            r"(sustained|di-2|no formal investigation merited|\bnot sustained\b|"
            r"exonerated|mediation|negotiated settlement|pending\b;? ?(investigation)?|"
            r"unfounded|duplicate investigation|withdrawn|rui\b|resigned under investigation"
            r"\bprescribed-sustained\b|greviance|investigation pending|cancelled|no disposition)"
        )
    )

    df.loc[:, "disposition"] = dispositions[0]
    return df


def extract_actions(df):
    actions = df.allegation.str.replace(
        r"(action ?(taken)?|discipline):? (na|none)", "no action", regex=True
    ).str.extract(
        r"no disposition|(\w+?-? ?d?a?y?s? suspension\b|none|hearing pending|"
        r"investigation pending|awaiting hearing|(withdrawn)? ?-? mediation|"
        r"rui -? ?(resigned ?(under investigation)?)?|"
        r"verbal counseling|rui - retired|"
        r"oral reprimand|none -nfim|n?s?a? ?-? letter of reprimand|none - exonerated|"
        r"duplicate allegation|dismissed|no action|remedial training|written documentation)"
    )
    df.loc[:, "action"] = actions[0].str.replace(r"sustained ", "", regex=False)
    return df


def extract_investigation_status(df):
    statuses = df.allegation.str.replace(r"(\w+) +$", r"\1", regex=True).str.extract(
        r"(forwarded$|completed$)"
    )
    df.loc[:, "investigation_status"] = statuses[0]
    return df


def extract_allegation_desc(df):
    allegation_desc = (
        df.allegation.str.replace(r"^ +?(\w+)", r"\1", regex=True)
        .str.replace(r"(\w+)  +(\w+)", r"\1 \2", regex=True)
        .str.replace(r"complaint tracking number:?", "", regex=True)
        .str.replace(r"performance of duty\b", "", regex=True)
        .str.replace(r"letter of reprimand\b", "", regex=True)
        .str.extract(
            r"(t?h?e? ?(detective|complaina?n?t?|officers?e?d?|accused"
            r"|employees?|victims?|supervisors|persons|man|evidence|technician|"
            r"destroying|public|counselor|investigation cancelled|inmates?).+)"
        )
    )
    df.loc[:, "allegation_desc"] = (
        allegation_desc[0]
        .str.replace(r"( disposition:?(.+)| discipline:?(.+))", "", regex=True)
        .str.replace(r"^mand ?", "", regex=True)
    )
    return df


def extract_allegation_made(df):
    allegations = (
        df.allegation.str.replace(r"(\w+) \/ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"orders ?[&\/]? ?directives", "orders and directives", regex=True)
        .str.replace(r"nfim", "no further investigation merited", regex=True)
        .str.replace(r"; professionalism", "; allegation: professionalsim", regex=False)
        .str.extract(r"allegations?:? (\w+ ?(\w+)? ?(\w+)? ?(\w+)? ?(\w+)?) ?;?")
    )
    df.loc[:, "allegation_made"] = allegations[0].str.replace(
        r"(duty|law|retaliation|professionalism).+", r"\1", regex=True
    )
    return df[~((df.tracking_id.fillna("") == ""))]


## before merge, make sure that tracking_id is effeciently extracted


def merge_2019_data(df):
    og_df = df[~df["tracking_id"].str.contains("2019")]

    df_2019 = df[df["tracking_id"].str.contains("2019")]

    dfa = df_2019[~((df_2019.investigation_status.fillna("") == ""))]
    dfa.loc[:, "receive_date"] = dfa.receive_date.fillna("")
    dfa.loc[:, "tracking_id"] = df.tracking_id.str.replace(r"-o", "-0", regex=False)

    badges = dfa.allegation.str.replace(
        r"(\w+)  +(\w+)", r"\1 \2", regex=True
    ).str.extract(
        r"(\w{3,5}) (di-2|not|sustained|exonerated|none"
        r"|greviance|rui|negotiated|unfounded|cancelled|duplicate|case|withdrawn)"
    )

    dfa.loc[:, "badge_no"] = badges[0]

    dfb = df_2019[~((df_2019.receive_date == ""))]

    clean_df_2019 = pd.merge(dfa, dfb, on="tracking_id", how="outer")

    clean_df_2019 = (
        clean_df_2019.drop(
            columns=[
                "investigation_status_y",
                "allegation_x",
                "allegation_y",
                "allegation_made_x",
                "allegation_made_y",
                "disposition_y",
                "action_y",
                "receive_date_x",
                "allegation_desc_x",
            ]
        )
        .rename(
            columns={
                "disposition_x": "disposition",
                "action_x": "action",
                "investigation_status_x": "investigation_status",
                "badge_no_x": "badge_no",
                "receive_date_y": "receive_date",
                "allegation_desc_y": "allegation_desc",
            }
        )
        .drop_duplicates(subset=["tracking_id", "badge_no"])
    )

    clean_df_2019 = clean_df_2019[~((clean_df_2019.badge_no.fillna("") == ""))]

    df = pd.concat([og_df, clean_df_2019], axis=0)
    return (
        df.drop(columns=["allegation"])
        .rename(columns={"allegation_made": "allegation"})
        .fillna("")
    )


def clean():
    df = (
        pd.read_csv(deba.data("ner/nopd_pib_reports_2014_2019.csv"))
        .pipe(stack_df)
        .pipe(clean_allegation)
        .pipe(extract_tracking_id)
        .pipe(extract_receive_date)
        .pipe(extract_disposition)
        .pipe(extract_actions)
        .pipe(extract_allegation_desc)
        .pipe(extract_allegation_made)
        .pipe(extract_investigation_status)
        .pipe(merge_2019_data)
        .pipe(names_to_title_case, ["tracking_id"])
        .pipe(set_values, {"agency": "New Orleans PD"})
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/cprr_new_orleans_pd_pib_reports_2014_2019.csv"), index=False)

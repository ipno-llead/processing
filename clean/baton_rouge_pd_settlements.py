import pandas as pd
from datetime import datetime
import deba
from lib.clean import clean_names, standardize_desc_cols
from lib.uid import gen_uid
from lib.columns import set_values


def convert_date_format(date_str):
    date_formats = ["%b %d, %Y", "%B %d, %Y", "%b %d %Y", "%B %d %Y"]

    for fmt in date_formats:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime("%m/%d/%Y")
        except ValueError:
            continue

    return date_str


def apply_date_conv(df):
    df.loc[:, "incident_date"] = df.incident_date.str.replace(
        r"Aug\. 18\, 2020", "08/18/2020", regex=True
    ).str.replace(r"Oct\. 29\, 2008", "10/29/2008", regex=True)
    df["claim_occur_date"] = df["incident_date"].apply(lambda x: convert_date_format(x))

    df.loc[:, "petition_receive_date"] = df.petition_receive_date.str.replace(
        r"Sept 18\, 2020", "09/18/2020", regex=True
    ).str.replace(r"Nov. 3\, 2009", "11/03/2009", regex=True)
    df["claim_receive_date"] = df["petition_receive_date"].apply(
        lambda x: convert_date_format(x)
    )

    df.loc[:, "claim_close_date"] = (
        df.close_date.str.replace(r"^(\w+), ", "", regex=True)
        .str.replace(r"Jun 9, 21", "06/09/2021", regex=False)
        .str.replace(r"Apr 6, 21", "04/06/2021", regex=True)
        .str.replace(r"Jun 8\, 22", "06/08/2022", regex=True)
        .str.replace(r"^24\, 2022$", "08/24/2022", regex=True)
        .str.replace(r"Mar 9\, 22", "03/09'2022", regex=True)
        .str.replace(r"Apr 6\, 22", "04/06/2022", regex=True)
        .str.replace(r"Feb 1\, 23", "02/01/2023", regex=True)
        .str.replace(r"Nov 9\, 22", "11/09/2022", regex=True)
        .str.replace(r"Sep 2\, 20", "09/02/2020", regex=True)
        .str.replace(r"Dec 9\, 20", "12/09/2020", regex=True)
        .str.replace(r"Jan 3\, 20", "01/03/2020", regex=True)
        .str.replace(r"Jan 8\, 20", "01/08/2020", regex=True)
        .str.replace(r"Sep 2\, 20", "09/02/2020", regex=True)
        .str.replace(r"\'(\w{4})$", r"/\1", regex=True)
    )
    df["claim_close_date"] = df["claim_close_date"].apply(lambda x: convert_date_format(x))

    return df.drop(columns=["close_date", "petition_receive_date", "incident_date"])


def clean_amount(df):
    df.loc[:, "amount"] = (
        df.amount.str.strip()
        .str.replace(r" ?(\w+) ?", r"\1", regex=True)
        .str.replace(r" ?\n ?", "", regex=True)
        .str.replace(r"\$", "", regex=True)
        .str.replace(r"\.(.+)", "", regex=True)
        .str.replace(r"\,", "", regex=True)
        .astype(int)
    )
    return df


def clean21():
    df = pd.read_csv(deba.data("raw/baton_rouge_pd/brpd-settlements-2021.csv"))
    df = (
        df.drop(columns=["Unnamed: 0"]).rename(columns={"case_no": "tracking_id"})
        .pipe(apply_date_conv)
        .pipe(clean_amount)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "baton-rouge-pd"})
        .pipe(standardize_desc_cols, ["claim_occur_date", "claim_receive_date", "claim_close_date"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["tracking_id", "claim_occur_date", "claim_close_date"], "settlement_uid")
    )
    return df


def clean22():
    df = pd.read_csv(deba.data("raw/baton_rouge_pd/brpd-settlements-2022.csv"))
    df = (
        df.rename(columns={"case_no": "tracking_id"})
        .pipe(apply_date_conv)
        .drop(columns=["officer"])
        .pipe(clean_amount)
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "baton-rouge-pd"})
        .pipe(standardize_desc_cols, ["claim_occur_date", "claim_receive_date", "claim_close_date"])
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["tracking_id", "claim_occur_date", "claim_close_date"], "settlement_uid")
    )
    return df


def clean20():
    df = pd.read_csv(deba.data("raw/baton_rouge_pd/brpd-settlements-2020.csv"))
    df = (
        df.rename(columns={"case_no": "tracking_id"})
        .pipe(apply_date_conv)
        .drop(columns=["Unnamed: 0", "middle_name"])
        .pipe(clean_amount)
        .pipe(standardize_desc_cols, ["claim_occur_date", "claim_receive_date", "claim_close_date"])
        .pipe(clean_names, ["first_name", "last_name"])
        .pipe(set_values, {"agency": "baton-rouge-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["tracking_id", "claim_occur_date", "claim_close_date"], "settlement_uid")
    )
    return df


def concat_dfs(dfa, dfb, dfc):
    df = pd.concat([dfa, dfb, dfc])
    return df 

if __name__ == "__main__":
    dfa = clean20()
    dfb = clean21()
    dfc = clean22()

    df = concat_dfs(dfa, dfb, dfc)
    df.to_csv(deba.data("clean/settlements_baton_rouge_pd_2020_2022.csv"), index=False)
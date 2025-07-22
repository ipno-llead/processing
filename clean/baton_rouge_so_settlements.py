import deba
import pandas as pd
from lib.columns import clean_column_names, set_values
from lib.clean import clean_dates, standardize_desc_cols
from lib.uid import gen_uid

def strip_leading_apostrophes(df: pd.DataFrame) -> pd.DataFrame:
    return df.applymap(lambda x: x.lstrip("'") if isinstance(x, str) else x)

def expand_and_parse_defendants(df: pd.DataFrame) -> pd.DataFrame:
    def parse_individual(name):
        name = name.strip().strip(',')

        # Handle common ranks — extend as needed
        ranks = ['Sheriff', 'Deputy', 'Detective', 'Warden']

        # Try to detect rank at beginning of string
        words = name.replace(',', '').split()
        rank = ''
        first = ''
        last = ''

        # If first word is a known rank
        if words and words[0] in ranks:
            rank = words[0]
            rest = words[1:]

            if len(rest) == 2:
                first, last = rest
            elif len(rest) == 1:
                last = rest[0]
        else:
            # No known rank — just parse name
            if len(words) == 2:
                first, last = words
            elif len(words) == 1:
                last = words[0]

        return {'rank_desc': rank, 'first_name': first, 'last_name': last}
    # Expand the dataframe
    expanded_rows = []
    for _, row in df.iterrows():
        defendant_list = str(row['defendants']).split(';')
        for defendant in defendant_list:
            parsed = parse_individual(defendant)
            new_row = row.to_dict()
            new_row.update(parsed)
            expanded_rows.append(new_row)

    return pd.DataFrame(expanded_rows)

def clean_settlement_amount(df: pd.DataFrame) -> pd.DataFrame:
    df['settlement_amount'] = df['settlement_amount'].replace('[\$,]', '', regex=True).astype(float)
    return df


def clean():
    df = (
        pd.read_csv(deba.data("raw/baton_rouge_so/east_baton_rouge_so_settlements_2021_2023.csv"))
        .pipe(clean_column_names)
        .rename(
            columns={
                "date":"claim_date",
                "total_amount_paid_by_sheriff": "settlement_amount",
            })
        .pipe(strip_leading_apostrophes)
        .pipe(expand_and_parse_defendants)
        .drop(columns=['defendants'])
        .pipe(clean_dates, ["claim_date"])
        .pipe(clean_settlement_amount)
        .pipe(standardize_desc_cols, ["rank_desc", "first_name", "last_name"]) 
        .pipe(set_values, {"agency": "east-baton-rouge-so"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(gen_uid, ["last_name", "settlement_amount", "uid"],"settlement_uid")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/settlements_baton_rouge_so_2021_2023.csv"), index=False)

import pandas as pd
import deba
from lib.columns import (
    rearrange_brady_columns,
)


if __name__ == "__main__":
    brady = pd.read_csv(deba.data("match/brady_baton_rouge_da_2021.csv"))
    brady_df = rearrange_brady_columns(brady)
    brady_df.to_csv(deba.data("fuse/brady_baton_rouge_da.csv"), index=False)

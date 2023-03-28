import pandas as pd
import deba
from lib.columns import (
    rearrange_brady_columns,
)


if __name__ == "__main__":
    brady = pd.read_csv(deba.data("clean/brady_tangipahoa_da_2021.csv"))
    brady_df = rearrange_brady_columns(brady)
    brady_df.to_csv(deba.data("fuse_agency/brady_tangipahoa_da.csv"), index=False)

import sys

sys.path.append("../")
from lib.path import data_file_path
from lib.columns import rearrange_brady_list_columns
import pandas as pd


if __name__ == "__main__":
    brady = pd.read_csv(data_file_path("match/brady_tangipahoa_da_2021.csv"))
    brady_df = rearrange_brady_list_columns(brady)
    brady_df.to_csv(data_file_path("fuse/brady_tangipahoa_da.csv"), index=False)

import sys

import pandas as pd
from datamatch import ThresholdMatcher, JaroWinklerSimilarity, ColumnsIndex

from lib.path import data_file_path, ensure_data_dir

sys.path.append('../')


if __name__ == '__main__':
    cprr = pd.read_csv(data_file_path('clean/cprr_new_orleans_so_2019.csv'))

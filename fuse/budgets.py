import deba
import pandas as pd
from lib.columns import (
    rearrange_docs_columns,
)



if __name__ == "__main__":
    docs = pd.read_csv(deba.data("clean/docs_budgets.csv"))
    # docs = rearrange_docs_columns(docs)
    # docs.to_csv(deba.data("fuse/docs_budgets.csv"), index=False)

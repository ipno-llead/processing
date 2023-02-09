def split_rows_with_multiple_labels(df):
    df = (
        df.drop("label", axis=1)
        .join(
            df["label"]
            .str.split("/", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("label"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    return df


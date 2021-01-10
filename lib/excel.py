def remove_unnamed_cols(df):
    return df[[col for col in df.columns if not col.startswith("Unnamed:")]]

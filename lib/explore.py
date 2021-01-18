def show_unique(df):
    print("%d rows" % len(df))
    for col in df.columns:
        print("%s:" % col)
        print("    dtype: %s" % df[col].dtype)
        s = df[col].unique()
        unique_len = len(s)
        print("    unique: %d" % unique_len)
        if unique_len < 100:
            print("        %s" % s)

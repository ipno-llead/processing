import pandas as pd
import deba
from lib.clean import clean_dates, clean_sexes, clean_races, standardize_desc_cols
from lib.columns import set_values
from lib.uid import gen_uid


def strip_apostrophes(df):
    for col in df.columns:
        if df[col].dtype == object:
            df.loc[:, col] = df[col].str.replace(r"^'", "", regex=True).str.strip()
    return df


def build_officer_lookup():
    """Parse officers CSV to build badge-to-name lookup."""
    df = pd.read_csv(
        deba.data("raw/denham_springs_pd/denham_springs_pd_uof_25_26_officers.csv"),
        header=0,
        usecols=[0, 1, 2, 3],
    )
    df.columns = ["datetime", "report_number", "officers_raw", "force_reason"]
    df = strip_apostrophes(df)

    # extract all "BADGE - LAST, FIRST" entries across all rows
    entries = []
    for raw in df.officers_raw.dropna():
        parts = pd.Series(raw).str.extractall(
            r"([A-Z]+\d+)\s*-\s*([A-Z]+),\s*([A-Z]+)"
        )
        for _, row in parts.iterrows():
            badge = row[0].strip().lower()
            last = row[1].strip().lower()
            first = row[2].strip().lower()
            entries.append({
                "badge_no": badge,
                "last_name": last,
                "first_name": first,
            })

    lookup = pd.DataFrame(entries).drop_duplicates(subset=["badge_no"])

    # also collect badge codes from the overview to find mismatches
    overview = pd.read_csv(
        deba.data("raw/denham_springs_pd/denham_springs_pd_uof_25_26_overview.csv"),
        header=0,
        usecols=[7],
    )
    overview.columns = ["officers_raw"]
    overview = strip_apostrophes(overview)
    overview_badges = set()
    for raw in overview.officers_raw.dropna():
        for b in raw.split(","):
            overview_badges.add(b.strip().lower())

    # for any overview badge not in lookup, try to find a lookup badge
    # that shares the same numeric suffix (handles typos like JP1709 vs JPI1709)
    import re
    extra = []
    for ob in overview_badges:
        if ob not in lookup.badge_no.values:
            num = re.search(r"\d+", ob)
            if num:
                matches = lookup[lookup.badge_no.str.contains(num.group() + "$")]
                if len(matches) == 1:
                    row = matches.iloc[0]
                    extra.append({
                        "badge_no": ob,
                        "last_name": row["last_name"],
                        "first_name": row["first_name"],
                    })

    if extra:
        lookup = pd.concat([lookup, pd.DataFrame(extra)], ignore_index=True)

    return lookup


def clean_overview():
    df = pd.read_csv(
        deba.data("raw/denham_springs_pd/denham_springs_pd_uof_25_26_overview.csv"),
        header=0,
        usecols=range(9),
    )
    df.columns = [
        "tracking_id", "datetime", "call_type", "force_reason",
        "citizen_demo", "use_of_force_type", "citizen_injured",
        "officers_raw", "officer_injured",
    ]
    df = strip_apostrophes(df)
    # filter out empty/garbage rows (OCR confidence scores)
    df = df[df.tracking_id.str.startswith("DP", na=False)].reset_index(drop=True)
    return df


def extract_datetime(df):
    parts = df.datetime.str.extract(
        r"^(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}:\d{2})"
    )
    df.loc[:, "occurred_date"] = parts[0]
    df.loc[:, "occurred_time"] = parts[1].str.zfill(5)
    return df.drop(columns=["datetime"])


def parse_citizen_demo(df):
    """Parse '37-M-W-H' into age, sex, race."""
    parts = df.citizen_demo.str.extract(r"^(\d+)-([A-Za-z])-([A-Za-z])")
    df.loc[:, "citizen_age"] = parts[0]
    df.loc[:, "citizen_sex"] = parts[1]
    df.loc[:, "citizen_race"] = parts[2]
    return df.drop(columns=["citizen_demo"])


def clean_force_type(df):
    df.loc[:, "use_of_force_type"] = (
        df.use_of_force_type.str.lower().str.strip()
    )
    return df


def clean_injured(df):
    df.loc[:, "use_of_force_result"] = (
        df.citizen_injured.str.lower()
        .str.strip()
        .str.replace(r"^$", "", regex=True)
    )
    return df.drop(columns=["citizen_injured"])


def clean_tracking_id_col(df):
    df.loc[:, "tracking_id_og"] = df.tracking_id.str.strip()
    return df


def split_and_merge_officers(df, lookup):
    """Split comma-separated officer badges, deduplicate, and merge names."""
    # explode officer badges into separate rows
    df = (
        df.drop("officers_raw", axis=1)
        .join(
            df["officers_raw"]
            .str.split(r"\s*,\s*", expand=True)
            .stack()
            .reset_index(level=1, drop=True)
            .rename("badge_no"),
            how="outer",
        )
        .reset_index(drop=True)
    )
    df.loc[:, "badge_no"] = df.badge_no.str.lower().str.strip()
    # deduplicate same officer on same incident
    df = df.drop_duplicates(subset=["tracking_id", "datetime" if "datetime" in df.columns else "occurred_date", "badge_no"])
    # merge names
    df = df.merge(lookup, on="badge_no", how="left")
    return df


def clean():
    lookup = build_officer_lookup()
    df = clean_overview()

    df = (
        df.pipe(split_and_merge_officers, lookup)
        .pipe(extract_datetime)
        .pipe(clean_dates, ["occurred_date"])
        .pipe(parse_citizen_demo)
        .pipe(clean_force_type)
        .pipe(clean_injured)
        .pipe(clean_tracking_id_col)
        .pipe(clean_sexes, ["citizen_sex"])
        .pipe(clean_races, ["citizen_race"])
        .pipe(
            standardize_desc_cols,
            ["incident_location"] if "incident_location" in df.columns else
            ["call_type", "force_reason", "badge_no"],
        )
        .drop(columns=["officer_injured"])
        .pipe(set_values, {"agency": "denham-springs-pd"})
        .pipe(gen_uid, ["first_name", "last_name", "agency"])
        .pipe(
            gen_uid,
            [
                "uid",
                "tracking_id",
                "use_of_force_type",
                "occurred_year",
                "occurred_month",
                "occurred_day",
            ],
            "uof_uid",
        )
        .pipe(gen_uid, ["tracking_id", "agency"], "tracking_id")
    )

    citizen_df = (
        df[["uof_uid", "citizen_age", "citizen_sex", "citizen_race", "agency"]]
        .copy()
        .pipe(
            gen_uid,
            ["citizen_age", "citizen_sex", "citizen_race", "agency"],
            "citizen_uid",
        )
        .drop_duplicates(subset=["citizen_uid", "uof_uid"])
    )

    uof_df = df[
        [
            "tracking_id",
            "tracking_id_og",
            "occurred_year",
            "occurred_month",
            "occurred_day",
            "occurred_time",
            "call_type",
            "force_reason",
            "use_of_force_type",
            "use_of_force_result",
            "badge_no",
            "first_name",
            "last_name",
            "agency",
            "uid",
            "uof_uid",
        ]
    ].drop_duplicates(subset=["uid", "uof_uid"])

    return uof_df, citizen_df


if __name__ == "__main__":
    uof, citizen_uof = clean()
    uof.to_csv(deba.data("clean/uof_denham_springs_pd_2025_2026.csv"), index=False)
    citizen_uof.to_csv(
        deba.data("clean/uof_cit_denham_springs_pd_2025_2026.csv"), index=False
    )

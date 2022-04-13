import pandas as pd
import deba
from lib.clean import (
    names_to_title_case,
    clean_names,
    clean_post_agency,
)
from lib.uid import gen_uid


def clean_post_names(df):
    names = (
        df.officer_name.str.lower()
        .str.strip()
        .str.extract(r"(\w+\'?\w+?)\,? (\w+) ?(\w+)?")
    )

    df.loc[:, "last_name"] = names[0]
    df.loc[:, "first_name"] = names[1]
    df.loc[:, "middle_name"] = names[2]
    return df


def split_agency(df):
    agency = (
        df.agency.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace("--bull-time", "full-time", regex=False)
        .str.replace("Ã¢â‚¬â€_-", " ", regex=False)
        .str.replace(" Ã¢â‚¬Ëœ ", "", regex=False)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency"] = agency[0]
    df.loc[:, "employment_status"] = agency[1]
    df.loc[:, "hire_date"] = agency[2]
    df.loc[:, "left_date"] = agency[3]
    df.loc[:, "left_reason"] = agency[4]

    agency1 = (
        df.agency_1.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_1"] = agency1[0]
    df.loc[:, "employment_status_1"] = agency1[1]
    df.loc[:, "hire_date_1"] = agency1[2]
    df.loc[:, "left_date_1"] = agency1[3]
    df.loc[:, "left_reason_1"] = agency1[4]

    agency2 = (
        df.agency_2.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_2"] = agency2[0]
    df.loc[:, "employment_status_2"] = agency2[1]
    df.loc[:, "hire_date_2"] = agency2[2]
    df.loc[:, "left_date_2"] = agency2[3]
    df.loc[:, "left_reason_2"] = agency2[4]

    agency3 = (
        df.agency_3.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_3"] = agency3[0]
    df.loc[:, "employment_status_3"] = agency3[1]
    df.loc[:, "hire_date_3"] = agency3[2]
    df.loc[:, "left_date_3"] = agency3[3]
    df.loc[:, "left_reason_3"] = agency3[4]

    agency4 = (
        df.agency_4.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_4"] = agency4[0]
    df.loc[:, "employment_status_4"] = agency4[1]
    df.loc[:, "hire_date_4"] = agency4[2]
    df.loc[:, "left_date_4"] = agency4[3]
    df.loc[:, "left_reason_4"] = agency4[4]

    agency5 = (
        df.agency_5.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace("jefferson parish so range 106 0", "", regex=False)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_5"] = agency5[0]
    df.loc[:, "employment_status_5"] = agency5[1]
    df.loc[:, "hire_date_5"] = agency5[2]
    df.loc[:, "left_date_5"] = agency5[3]
    df.loc[:, "left_reason_5"] = agency5[4]

    agency6 = (
        df.agency_6.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_6"] = agency6[0]
    df.loc[:, "employment_status_6"] = agency6[1]
    df.loc[:, "hire_date_6"] = agency6[2]
    df.loc[:, "left_date_6"] = agency6[3]
    df.loc[:, "left_reason_6"] = agency6[4]

    agency7 = (
        df.agency_7.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_7"] = agency7[0]
    df.loc[:, "employment_status_7"] = agency7[1]
    df.loc[:, "hire_date_7"] = agency7[2]
    df.loc[:, "left_date_7"] = agency7[3]
    df.loc[:, "left_reason_7"] = agency7[4]

    agency8 = (
        df.agency_8.astype(str)
        .str.lower()
        .str.strip()
        .str.replace(r"(\w+) = (\w+)", r"\1 \2", regex=True)
        .str.replace(
            r"^orleans parish coroner\'s office", "orleans coroners office", regex=True
        )
        .str.replace(r"(\w+) Ã¢â‚¬â€ (\w+)", r"\1 \2", regex=True)
        .str.replace(r"1st parish court", "first court", regex=False)
        .str.replace(r"(\w+) Ã‚Â© (\w+)\/(\w+)\/(\w+)", r"\1 \2/\3/\4", regex=True)
        .str.replace(r" Â© ", "", regex=True)
        .str.replace(r"--BULL-TIME", "FULL-TIME", regex=True)
        .str.replace(r"_(\w+)\/(\w+)\/(\w+)â€”_", r"\1/\2/\3", regex=True)
        .str.replace(r" â€”= ", "", regex=True)
        .str.replace(r" +", " ", regex=True)
        .str.replace(r" & ", "", regex=True)
        .str.replace(r" - ", "", regex=True)
        .str.replace(r" _?â€\” ", "", regex=True)
        .str.replace(r" _ ", "", regex=True)
        .str.replace(r" = ", "", regex=True)
        .str.replace(r"^ (\w+)", r"\1", regex=True)
        .str.replace(r"^st (\w+)", r"st\1", regex=True)
        .str.replace(r" of ", "", regex=True)
        .str.replace(r" p\.d\.", " pd", regex=True)
        .str.replace(r" pari?s?h? so", " so", regex=True)
        .str.replace(r" Â§ ", "", regex=True)
        .str.replace(r" â€˜", "", regex=True)
        .str.replace(r"(\.|\,)", "", regex=True)
        .str.replace(r"miss river", "river", regex=False)
        .str.replace(r" ~ ", "", regex=False)
        .str.replace(r"(\w+) _ (\w+)", r"\1 \2", regex=True)
        .str.replace("new orleans harbor", "orleans harbor", regex=False)
        .str.replace(
            "probation & parcole - adult",
            "adult probation",
            regex=True,
        )
        .str.extract(
            r"^(\w{1}? ?\w+ ?\w+? ?\w+) ?(deceased|full-time|reserve|retired|part-time)? ? "
            r"?(\w{1,2}\/\w{1,2}\/\w{4}) ? ?(\w{1,2}\/\w{1,2}\/\w{4})? ?(termination|(\w+)? "
            r"?(resignation)|(\w+)? ?(resigned)|retired)?"
        )
    )
    df.loc[:, "agency_8"] = agency8[0]
    df.loc[:, "employment_status_8"] = agency8[1]
    df.loc[:, "hire_date_8"] = agency8[2]
    df.loc[:, "left_date_8"] = agency8[3]
    df.loc[:, "left_reason_8"] = agency8[4]
    return df


def drop_missing_agency(df):
    return df[~((df.agency.fillna("") == ""))]


def drop_missing_matched_uid(df):
    return df[~(df.matched_uid.fillna("") == "")]


def clean():
    df = (
        pd.read_csv(deba.data("raw/post/post_officer_history.csv"))
        .pipe(clean_post_names)
        .pipe(clean_names, ["first_name", "middle_name", "last_name"])
        .pipe(split_agency)
        .pipe(
            names_to_title_case,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(
            clean_post_agency,
            [
                "agency",
                "agency_1",
                "agency_2",
                "agency_3",
                "agency_4",
                "agency_5",
                "agency_6",
                "agency_7",
                "agency_8",
            ],
        )
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency"])
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_1"], "uid_1")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_2"], "uid_2")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_3"], "uid_3")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_4"], "uid_4")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_5"], "uid_5")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_6"], "uid_6")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_7"], "uid_7")
        .pipe(gen_uid, ["first_name", "last_name", "middle_name", "agency_8"], "uid_8")
    )
    return df


if __name__ == "__main__":
    df = clean()
    df.to_csv(deba.data("clean/post_officer_history.csv"), index=False)

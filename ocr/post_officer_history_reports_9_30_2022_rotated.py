import deba
import pandas as pd
from lib.dvc import real_dir_path
from lib.ocr import process_pdf

def only_pdf(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.filetype == "pdf"].reset_index(drop=True)


def process_pdfs_2023_rotated() -> pd.DataFrame:
    dir_name = real_dir_path("raw_post_officer_history_reports_9_30_2022_rotated.dvc")
    return (
        pd.read_csv(deba.data("meta/post_officer_history_reports_9_30_2022_rotated_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name)
    )


if __name__ == "__main__":
    df23_rotated = process_pdfs_2023_rotated()
    df23_rotated.to_csv(deba.data("ocr/post_officer_history_reports_9_30_2022_rotated_pdfs.csv"), index=False)
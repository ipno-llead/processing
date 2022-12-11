import deba
import pandas as pd
from lib.dvc import real_dir_path
from lib.ocr import process_pdf


def only_pdf(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[df.filetype == "pdf"].reset_index(drop=True)


def process_all_pdfs() -> pd.DataFrame:
    dir_name = real_dir_path("raw_new_orleans_pd_appeals.dvc")
    return (
        pd.read_csv(deba.data("meta/new_orleans_pd_appeals_files.csv"))
        .pipe(only_pdf)
        .pipe(process_pdf, dir_name, output_images_to_tempdir=False)
    )


if __name__ == "__main__":
    df = process_all_pdfs()
    df.to_csv(deba.data("ocr/new_orleans_pd_appeals_pdfs.csv"), index=False)

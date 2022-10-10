from this import d
import dropbox
from dropbox.files import FolderMetadata, FileMetadata
import deba
import pandas as pd
from lib.dvc import files_meta_frame


def download_db_metadata():
    with open("db.txt", "r") as f:
        access_token = f.read()
    dbx = dropbox.Dropbox(access_token)

    # get meta_data
    print(
        "...authenticated with Dropbox owned by "
        + dbx.users_get_current_account().name.display_name
    )
    result = dbx.files_list_folder(
        r"/IPNO/data/Louisiana State/Data Collected/Louisiana State Police Department"
        r"/Data Collected/Disciplinary Letters/2019/raw_louisiana_state_pd_letters_2019",
        recursive=True,
    )

    file_list = []

    def process_entries(entries):
        for entry in entries:
            if isinstance(entry, dropbox.files.FileMetadata):
                file_list.append([entry])

    process_entries(result.entries)

    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        process_entries(result.entries)

    print(len(file_list))
    df = pd.DataFrame.from_records(file_list, columns=["meta_data"])
    return df


def set_filetype(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[df.filepath.str.lower().str.endswith(".pdf"), "filetype"] = "pdf"
    return df


def split_filepath(df: pd.DataFrame) -> pd.DataFrame:
    filepaths = df.filepath.str.split("/")
    df.loc[:, "fn"] = filepaths.map(lambda v: v[:])
    return df


def set_file_category(df: pd.DataFrame) -> pd.DataFrame:
    column = "file_category"
    df.loc[df.fn.astype(str).str.match(r"(.+)"), column] = "louisiana_state_pd_letter"
    return df


def fetch_reports() -> pd.DataFrame:
    return (
        files_meta_frame("raw_louisiana_state_pd_letters_2019.dvc")
        .pipe(set_filetype)
        .pipe(split_filepath)
        .pipe(set_file_category)
    )


if __name__ == "__main__":
    df = fetch_reports()
    db_meta = download_db_metadata()
    df.to_csv(deba.data("meta/letters_louisiana_state_pd_2019_files.csv"), index=False)
    db_meta.to_csv(
        deba.data("meta/letters_louisiana_state_pd_2019_db_files.csv"), index=False
    )

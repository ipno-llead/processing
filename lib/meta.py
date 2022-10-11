import dropbox
import pandas as pd


def download_db_metadata(fpath: str) -> pd.DataFrame:
    with open("db.txt", "r") as f:
        access_token = f.read()
    dbx = dropbox.Dropbox(access_token)

    # get meta_data
    print(
        "...authenticated with Dropbox owned by "
        + dbx.users_get_current_account().name.display_name
    )
    result = dbx.files_list_folder(
        fpath,
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
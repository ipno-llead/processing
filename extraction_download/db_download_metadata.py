import dropbox
import deba
import pandas as pd


def download_db_metadata():
    with open(deba.data("raw/post/token/token.txt")) as f:
        access_token = f.read()
    dbx = dropbox.Dropbox(access_token)

    # get meta_data
    print(
        "...authenticated with Dropbox owned by "
        + dbx.users_get_current_account().name.display_name
    )
    result = dbx.files_list_folder(
        "/IPNO/data/POST/Data Collected/reports/All",
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


if __name__ == "__main__":
    df = download_db_metadata()
    df.to_csv(deba.data("raw/post/metadata/post_metadata.csv"), index=False)

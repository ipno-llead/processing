import dropbox
from lib.path import data_file_path
import pandas as pd


with open(data_file_path("drop_box/token.txt", "r")) as f:
    access_token = f.read()


dbx = dropbox.Dropbox(access_token)


# get meta_data
print(
    "...authenticated with Dropbox owned by "
    + dbx.users_get_current_account().name.display_name
)
result = dbx.files_list_folder(
    "/IPNO/data/Orleans/Data Collected/New Orleans Civil Service Commission/transcripts/",
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
df.to_csv(
    data_file_path(
        "raw/new_orleans_pd/appeal_hearing_pdfs/appeal_hearing_pdfs_meta_data.csv"
    ),
    index=False,
)

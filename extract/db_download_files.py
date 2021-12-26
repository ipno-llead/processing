import dropbox
from lib.get_dropbox import get_files, get_folders
from lib.path import data_file_path


with open(data_file_path("drop_box/token.txt", "r")) as f:
    access_token = f.read()


dbx = dropbox.Dropbox(access_token)


folders = get_folders(
    dbx, "/IPNO/data/Orleans/Data Collected/New Orleans Civil Service Commission/"
)

# print folder IDs
print(folders)

# # folder_id for transcripts folder within the directory above
folder_id = "id:8ceKnrnmgi0AAAAAAAA5bA"

# download directory
download_dir = data_file_path("raw/new_orleans_pd/appeal_hearing_pdfs/")

# # print('Obtaining list of files in target directory...')
get_files(dbx, folder_id, download_dir)

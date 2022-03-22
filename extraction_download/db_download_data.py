import dropbox
from lib.get_dropbox import get_files, get_folders
import deba


def download_db_files():
    with open(deba.data("raw/post/token/token.txt")) as f:
        access_token = f.read()

    dbx = dropbox.Dropbox(access_token)

    folders = get_folders(dbx, "/IPNO/data/POST/Data Collected/reports/All")

    # print folder IDs
    print(folders)

    # folder_id for transcripts folder within the directory above
    folder_id = "id:8ceKnrnmgi0AAAAAAABEAg"

    # download directory
    download_dir = deba.data("raw/post/download/")

    # print('Obtaining list of files in target directory...')
    get_files(dbx, folder_id, download_dir)


if __name__ == "__main__":
    download_db_files()

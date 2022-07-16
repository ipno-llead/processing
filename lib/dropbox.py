import os
from typing import List, Tuple

import dropbox
from dotenv import load_dotenv
from tqdm import tqdm

from lib.dropbox_hasher import DropboxContentHasher


def _compute_content_hash(file_path: str):
    hasher = DropboxContentHasher()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(4 * 1024)
            if len(chunk) == 0:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def sync_local_to_dropbox(
    local_root: str, db_root: str, file_paths: List[str], dry_run: bool = False
) -> List[Tuple[str, str, str]]:
    """Sync files from local directory to a dropbox path

    New files will be created, while existing files will be updated if
    the content hash doesn't match.

    This function reads Dropbox token from DROPBOX_TOKEN env var, which
    can be provided via .env.

    Args:
        local_root (str):
            the local directory that contains files to sync to Dropbox
        db_root (str):
            Dropbox root path to sync to. Final dropbox paths are db_root
            joined with file_paths
        file_paths (List[str]):
            paths of files to sync to Dropbox, relative to local_root
        dry_run (bool):
            when set to True, print upload/delete statements instead of
            executing them

    Returns:
        a list of tuples of 3 elements for each file:
            dropbox_path, dropbox_id, content_hash
    """
    load_dotenv()
    try:
        db_token = os.environ["DROPBOX_TOKEN"]
    except KeyError:
        raise Exception("DROPBOX_TOKEN missing in .env file")
    dbx = dropbox.Dropbox(db_token)

    result = dbx.files_list_folder(db_root, recursive=True)
    db_files = dict()
    while True:
        for entry in result.entries:
            db_files[entry.path_display[len(db_root) :].lstrip("/")] = entry
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)

    result = []
    for fp in tqdm(file_paths, desc="Syncing files to Dropbox", total=len(file_paths)):
        file_path = os.path.join(local_root, fp)
        content_hash = _compute_content_hash(file_path)
        if fp in db_files and content_hash == db_files[fp].content_hash:
            fmd = db_files[fp]
            result.append((fmd.path_display, fmd.id, content_hash))
            continue
        db_file_path = os.path.join(db_root, fp)
        if dry_run:
            print(
                "dbx.files_upload(%s, %s, content_hash=%s)"
                % (file_path, db_file_path, content_hash)
            )
        else:
            with open(file_path, "rb") as f:
                fmd = dbx.files_upload(
                    f.read(), db_file_path, content_hash=content_hash
                )
            result.append((fmd.path_display, fmd.id, content_hash))

    return result

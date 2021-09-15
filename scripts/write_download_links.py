#!/usr/bin/env python

import errno
import shutil
import os
import argparse
import pathlib
import json
import re
from urllib.parse import urlparse, unquote


def filename_from_url(url):
    o = urlparse(url)
    filename = unquote(o.path).split("/")[-1].lower()
    return re.sub(r'\s+', '_', filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=(
        'Extract CSV NAME from each link found in json file and write link to NAME.link files. '
        'It will only update NAME.link files for links that have changed.'
    ))
    parser.add_argument(
        'links_file', type=pathlib.Path, metavar='LINKS_FILE',
        help='Read links from this file (must be a JSON file).',
    )
    parser.add_argument(
        'link_dir', type=pathlib.Path, metavar='LINK_DIR',
        help='Write NAME.link files to this folder.',
    )
    args = parser.parse_args()
    if not args.links_file.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args.links_file
        )
    if not args.link_dir.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args.link_dir
        )
    if not args.link_dir.is_dir():
        raise NotADirectoryError(
            errno.ENOTDIR, os.strerror(errno.ENOTDIR), args.link_dir
        )

    with open(args.links_file) as f:
        dirs = json.load(f)

    for dir_name, links in dirs.items():
        link_dir = args.link_dir / dir_name
        try:
            os.mkdir(link_dir, 0o755)
        except FileExistsError:
            pass

        # add new links/update existing links
        names = set()
        for link in links:
            name = filename_from_url(link)
            fp = link_dir / ('%s.link' % name)
            names.add(fp.name)
            link_match = False
            try:
                with open(fp, 'r') as f:
                    if f.read().strip() == link:
                        link_match = True
            except FileNotFoundError:
                pass
            if not link_match:
                with open(fp, 'w') as f:
                    f.write(link)

        # remove files that are no longer found in links_file
        for file in link_dir.iterdir():
            if not file.is_dir() and file.name not in names:
                os.remove(file)

    for subdir in args.link_dir.iterdir():
        if subdir.is_dir() and subdir.name not in dirs:
            shutil.rmtree(subdir, ignore_errors=True)

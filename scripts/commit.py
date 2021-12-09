# temporary script that will be retired once wrgl fleet is out (replace wrgl sync)

import argparse
import sys
import subprocess
import pathlib
import datetime

import yaml

from lib.path import data_file_path

sys.path.append("../")


def get_previous_commit_time(repo_dir: str, branch: str) -> datetime.datetime:
    output = subprocess.check_output(
        [
            "/bin/sh", "-c",
            "wrgl log %s -P --wrgl-dir %s | grep Date" % (branch, repo_dir),
        ],
        stderr=subprocess.STDOUT,
        encoding='utf8'
    )
    if output.strip() != 'key not found':
        return datetime.datetime.strptime(
            output.splitlines()[0].strip(),
            '%Y-%m-%d %H:%M:%S %z %z'
        )
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Commit if data file is newer than commit time'
    )
    parser.add_argument(
        'repo_name', type=str, help='must match repo name found in data/wrglsync.yaml'
    )
    parser.add_argument(
        'branch', type=str, help='the branch to compare against and commit to'
    )
    args = parser.parse_args()

    wrglsync_path = data_file_path('.wrglsync.yaml')
    with open(wrglsync_path, 'r') as f:
        data = yaml.load(f)
        username = data['username']
        try:
            repo = data['repos'][args.repo_name]
        except KeyError as e:
            raise KeyError('repo %s not found: %s' % (args.repo_name, e))

    repo_dir = pathlib.Path(__file__).parent.parent / '.wrglfleet' / args.repo_name
    file_path = wrglsync_path.parent / repo['fileName']
    com_time = get_previous_commit_time(repo_dir, args.branch)
    file_mod = datetime.datetime.fromtimestamp(file_path.stat().st_mtime)
    if com_time is None or com_time < file_mod:
        subprocess.run([
            'wrgl',
            'commit',
            '--wrgl-dir',
            repo_dir,
            args.branch,
            file_path,
            'commit with scripts/commit.py',
            '-p',
            ','.join(repo['primaryKey'])
        ])
    else:
        print('commit is newer than file, skipping')

#!/usr/bin/env python

import argparse
import pathlib
import json
import errno
import os
import csv
import re


selection_marks_regex = re.compile(r"\s+:(un)?selected:")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extract tables from Azure Form Recognizer JSON and save as CSV files.')
    parser.add_argument(
        'json_file', type=pathlib.Path, metavar='JSON_FILE',
        help='JSON result from Azure Form Recognizer',
    )
    parser.add_argument(
        'csv_path', type=str, metavar='CSV_PATH',
        help=(
            'Path pattern that will be appended with table index and ".csv" to generate the CSV save paths. '
            'I.e., if JSON_FILE contains two tables and CSV_PATH is "/data/my_doc" then the results '
            'will be saved to "/data/my_doc_00.csv" and "/data/my_doc_01.csv"'
        )
    )
    parser.add_argument(
        '--trim-selection-marks', action='store_true',
        help='remove ":selected:" and ":unselected:" sequences from cell values'
    )
    args = parser.parse_args()
    if not args.json_file.exists():
        raise FileNotFoundError(
            errno.ENOENT, os.strerror(errno.ENOENT), args.json_file
        )
    with open(args.json_file) as f:
        data = json.load(f)
    for tbl_idx, tbl in enumerate(data['tables']):
        rows = [[''] * tbl['columnCount'] for _ in range(tbl['rowCount'])]
        for cell in tbl['cells']:
            content = cell['content'].strip()
            if args.trim_selection_marks:
                cell['content'] = selection_marks_regex.sub('', content)
            rows[cell['rowIndex']][cell['columnIndex']] = cell['content']
        with open("%s_%02d.csv" % (args.csv_path.strip(), tbl_idx), 'w') as f:
            w = csv.writer(f)
            w.writerows(rows)

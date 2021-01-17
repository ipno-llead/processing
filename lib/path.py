import os
import pathlib

_current_dir = os.path.dirname(os.path.realpath(__file__))


def data_file_path(filepath):
    return os.path.join(_current_dir, "../data", filepath)


def ensure_data_dir(name):
    pathlib.Path(data_file_path(name)).mkdir(parents=True, exist_ok=True)

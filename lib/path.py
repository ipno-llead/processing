import os
import pathlib

_current_dir = os.path.dirname(os.path.realpath(__file__))


def data_file_path(filepath: str) -> pathlib.Path:
    """Gets full file path of file in data folder
    """
    return pathlib.Path(_current_dir, "../data", filepath).resolve()


def ensure_data_dir(name: str) -> None:
    """Ensure directory is created under data folder
    """
    pathlib.Path(data_file_path(name)).mkdir(parents=True, exist_ok=True)

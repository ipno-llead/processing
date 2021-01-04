# PPACT data processing

Notebooks and Python scripts to process PPACT data.

## Getting started

You can start with any popular way to run Jupyter notebooks. Some recommended ways are:

- [Anaconda](https://www.anaconda.com/)
- [Jupyter extension for VSCode](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)

If in doubt about package versions, install from `requirements.txt` with `pip` or `conda`:

```bash
pip install -r requirements.txt # OR
conda install --file requirements.txt
```

## Project layout

Main folders are:

- **data**: Input data goes in here. Data are not committed however. Can be populated by hand or preferably via scripts.
- **lib**: Reusable Python functions are stored here.
- **notebooks**: Keep all Jupyter notebooks. For now this folder should be flat, no subfolder.
- **output**: Some notebooks output CSV files for various purposes. All such files should go in here.

## Glossary

### Agencies

- **PD**: Police Department
- **SO**: Sheriff’s Office
- **CSD**: Civil Service Department
- **CSC**: Civil Service Commission

### Request Types

- **PPRR**: Personnel Public Records Request
- **CPPR**: Complaints Public Records Request

# PPACT data integration

Notebooks and Python scripts to combine & integrate PPACT data. All generated data are kept in [WRGL](https://www.wrgl.co/@ipno).

## Install Jupyter & Python dependencies

You can start with any popular way to run Jupyter notebooks. Some recommended ways are:

- [Anaconda](https://www.anaconda.com/)
- [Jupyter extension for VSCode](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)

If in doubt about package versions, install from `requirements.txt` with `pip` or `conda`:

```bash
pip install -r requirements.txt # OR
conda install --file requirements.txt
```

## Get started

```bash
# Download input data files from Dropbox
scripts/download_input.py
# process all data files, check `data` folder for all generated files.
make
# combine all data into personnel, personnel_history and complaint data
scripts/run.sh fuse/all.py
```

## Workflow

Every data integration workflow including this one has these steps:

1. **Standardization & cleaning**: The first and most important step. If this produce inaccurate data, subsequent steps cannot save it.
   - **Data realignment**: If data come from an OCR system such as Textract, this step correct any column & row mis-alignment problem. Also correct any typo introduced by OCR.
   - **Schema matching**: Identify and give columns with the same meaning the same name across all datasets. Condense whitespace, strip and remove unwanted characters and make sure each and every column names are the same case.
   - **Data cleaning**: For each column, remove unwanted characters, condense whitespace.
   - **Data standardization**: Make sure values with the same meaning are written the same in every dataset. This apply to both categorical data (such as officer's rank) and more free-form data such as locality, street name, etc.
   - **Data tokenization & segmentation**: This step is needed for columns which hold multiple pieces of information (such as `full_name`, `full_address`) and which doesn't have clearly delineated structure. For each value, a set of possible tags should be applied to the characters, which can then become the basis to segment this column into multiple output columns.
   - **Output verification**: Possible if another source can be consulted. This step ensure the output quality which is important for subsequent steps to also produce high quality output.
2. **Data matching**: Compare data between 2 datasets and see if there are matches. Or if there are duplications in a single dataset this step still need to be carried out as part of deduplication effort.
   - **Indexing**: If one naively compare each record to every other record then time complexity will become quadratic and make this step incredibly slow. This step select an index key to divide each dataset into multiple subgroups of data such that similar records are grouped together and there are as few records in each subgroup as possible. Detailed comparison between records will only happen between records in the same subgroup.
   - **Record comparison**: For every pair of potential match, each field is compared using a function which produce a number between 0 and 1 (0 denote total non-match while 1 mean exactly the same). The output of this step is a numeric comparison vector for every records pairs.
   - **Classification**: Based on the comparison vector, each records pairs are classified into: matches, non-matches or potential-matches.
3. **Data fusion**: At this stage data maybe split or joined to match output schema. Additional columns may be added if they can be figured out from existing columns (i.e. `middle_initial` from `middle_name`).

Each major step above should output CSV files which can then be saved in a versioning system such as [WRGL](https://www.wrgl.co) to keep track of how the process and output data evolve over time.

## Project layout

- **scripts**: Scripts that can be called directly go here.
  - **download_input.py**: Download relevant input files from Dropbox to folder `data` if they don't already exist.
  - **test.sh**: Run tests against `lib/` folder.
- **clean**: This folder correspond to the `Standardization & cleaning` step above. Each module should be dedicated to clean only 1 file or a group of files with similar format (i.e. yearly data from a single police department). Each module take input data from `data` folder and output CSV files back to `data/clean` with name like `{data_type}_{agency_name}_{optional_date}.csv`.
- **match**: This folder correspond to the `Data matching` step. Each module take cleaned data from the previous step, do any matching and save to `data/match` folder with name like `{data_type}_{agency_name}_{optional_date}.csv`.
- **fuse**: This folder correspond to the `Data fusion` step. Data files from previous steps are combined and saved to `data/fuse` folder with name like `{data_type}.csv` (agency name and date info should be present in the data itself).
- **lib**: Any shared Python code go in here.
- **references**: Any data table kept for consultation in standardization step live here.
- **data**: Data files downloaded from Dropbox or produced by scripts go here.
- **notebooks**: Keep all Jupyter notebooks for exploration or demonstration purpose.
- **Makefile**: Contain file & processing dependencies. Anytime data or code change, run `make` once to re-process only files which might be affected.

## Naming conventions

- Each Python function and Python module name should be in `snake_case` (e.g. `clean_pprr_jean_lafitte_2016.py`).
- Each output CSV name should also be in `snake_case` with format unique to each step like described in previous section.
- Possible `data_type` that might appear in file names:
  - **pprr**: Personnel Public Records Request.
  - **cprr**: Complaints Public Records Request.
- WRGL repo prefixes:
  - **per-**: Personnel data.
  - **perhist-**: Personnel history data.
  - **com-**: Complaint data.
- Abbreviations that might appear in file names and agency names:
  - **pd**: Police Department
  - **so**: Sheriff’s Office
  - **csd**: Civil Service Department
  - **csc**: Civil Service Commission

## Run tests

```bash
scripts/test.sh
```

## Reviewing matching results

Each script in `match` folder produce one or more `.xlsx` files in `data/match` folder showing matched records in an easy-to-review format. The naming convention for those Excel files is `{agency}_{source_a}_v_{source_b}.xlsx`. For example `new_orleans_harbor_pd_cprr_2020_v_pprr_2020.xlsx` shows matched records between `New Orleans Harbor PD CPRR 2020` and `New Orleans Harbor PD PPRR 2020` datasets. Each Excel files has 3 sheets:

- **Sample pairs**: Show a small sample of record pairs for each similarity score range.
- **All pairs**: Show all pairs of records and their respective similarity score for a more in-depth review. Pairs that score too low (and therefore could never be considered match anyway) are not present.
- **Decision**: Has 2 values
  - `match_threshold`: the cut-off similarity score that the matcher has decided on. Everything below this score is considered non-match
  - `number_of_matched_pairs`: the number of matched pairs using this threshold.

## Validate output

```bash
pip install datavalid
python -m datavalid --dir data
```

## Regenerate schema.md from datavalid.yml file

```bash
python -m datavalid --dir data --doc schema.md
```

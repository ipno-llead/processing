# PPACT data integration

Notebooks and Python scripts to combine & integrate PPACT data.

## Install Jupyter & Python dependencies

You can start with any popular way to run Jupyter notebooks. Some recommended ways are:

- [Anaconda](https://www.anaconda.com/)
- [Jupyter extension for VSCode](https://marketplace.visualstudio.com/items?itemName=ms-toolsai.jupyter)

If in doubt about package versions, install from `requirements.txt` with `pip` or `conda`:

```bash
pip install -r requirements.txt # OR
conda install --file requirements.txt
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

- **scripts**: Scripts that can be called directly go here. This folder contains entry to the whole workflow.
- **clean**: This folder correspond to the `Standardization & cleaning` step above. Each module should be dedicated to clean only 1 file or a group of files with similar format (i.e. yearly data from a single police department). Each module take input data from `data` folder and output CSV files to `output` folder with name like `clean_{data_type}_{agency_name}_{optional_date}.csv`.
- **match**: This folder correspond to the `Data matching` step. Each module take cleaned data from the previous step, do any matching and save to `output` folder with name like `match_{data_type}_{agency_name}_{optional_date}.csv`.
- **fuse**: This folder correspond to the `Data fusion` step. Data files from previous steps are combined and saved to `output` folder with name like `fuse_{data_type}.csv` (agency name and date info should be present in the data itself).
- **lib**: Any shared Python code go in here.
- **references**: Any data table kept for consultation in standardization step live here.
- **data**: Input data go here. This folder is ignored and should never appear in Git repo. Can be populated by hand or preferably via scripts.
- **output**: Output of any step are saved here for manual inspection and/or to be moved to a data versioning system. This folder is also never commited to Git.
- **notebooks**: Keep all Jupyter notebooks for exploration or demonstration purpose.

## Naming conventions

- Each Python function and Python module name should be in `snake_case` (e.g. `clean_pprr_jean_lafitte_2016.py`).
- Each output CSV name should also be in `snake_case` with format unique to each step like described in previous section.
- Possible `data_type` that might appear in file names:
  - **pprr**: Personnel Public Records Request.
  - **cprr**: Complaints Public Records Request.
  - **personel**: Unique personel list.
  - **personel_hist**: Personel history.
- Abbreviations that might appear in file names and agency names:
  - **pd**: Police Department
  - **so**: Sheriff’s Office
  - **csd**: Civil Service Department
  - **csc**: Civil Service Commission

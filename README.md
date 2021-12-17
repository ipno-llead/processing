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
# initialize wrgl repo if you haven't
wrgl init

# pull all repositories
wrgl pull --all

# rerun processing pipeline as necessary
make

# check whether the output match schema with datavalid
python -m datavalid --dir data

# show changes for all
wrgl diff --all

# show in-depth changes for a single branch
wrgl diff event

# commit a single repo
wrgl commit event

# commit all branches
wrgl commit --all "my new commit"

# log-in with your wrglhub credentials
wrgl credentials authenticate https://hub.wrgl.co/api

# push all changes
wrgl push --all

# push a single repository
wrgl push event
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

- **raw_datasets.json**: Links to download raw input files organized under agency names.
- **scripts**: Scripts that can be called directly go here.
  - **rawfiles.sh**: Download and display raw input files for a certain agency (list of agencies is in raw_datasets.json). Useful when starting write a script
  - **test.sh**: Run tests against `lib/` folder.
  - **run.sh**: Run a script with PYTHONPATH set to proper value.
- **clean**: This folder correspond to the `Standardization & cleaning` step above. Each module should be dedicated to clean only 1 file or a group of files with similar format (i.e. yearly data from a single police department). Each module take input data from `data` folder and output CSV files back to `data/clean` with name like `{data_type}_{agency_name}_{optional_date}.csv`.
- **match**: This folder correspond to the `Data matching` step. Each module take cleaned data from the previous step, do any matching and save to `data/match` folder with name like `{data_type}_{agency_name}_{optional_date}.csv`.
- **fuse**: This folder correspond to the `Data fusion` step. Data files from previous steps are combined and saved to `data/fuse` folder with name like `{data_type}.csv` (agency name and date info should be present in the data itself).
- **lib**: Any shared Python code go in here.
- **references**: Any data table kept for consultation in standardization step live here.
- **data**: Data files downloaded from Dropbox or produced by scripts go here.
  - **raw**: Raw data input files
  - **clean**: Output of clean scripts
  - **match**: Output of match scripts
  - **fuse**: Output of fuse scripts
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

## Extract tables from PDFs with Form Recognizer

This repository proposes a workflow and some utilty scripts to help extracting tables with [Azure From Recognizer](https://azure.microsoft.com/en-us/services/form-recognizer/). There are 2 workflows:

### For simple table extraction

You can simply use one of FR's prebuilt model, specifically the [Layout model](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/concept-layout). Just use the web-based FormRecognizerStudio to upload documents and extract data. If there are too many pages to extract manually, we can add the ability to automate extraction from prebuilt models to `scripts/extract_tables_from_doc.py`.

### For complex table extraction

You need to train a custom model and use that model to extract data. Follow these steps:

1. Create a `.env` file (see [python-dotenv](https://pypi.org/project/python-dotenv/) to learn the syntax) at the root directory of this repository with the following environment variable:
   - `FORM_RECOGNIZER_ENDPOINT`: follow [these instruction](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/try-v3-python-sdk#prerequisites) to get endpoint and key for a Form Recognizer resource.
   - `FORM_RECOGNIZER_KEY`: see above.
   - `BLOB_STORAGE_CONNECTION_STRING`: create an Azure storage account to store training data and follow this [guide](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) to get the connection string.
   - `FORM_RECOGNIZER_CONTAINER`: create a container in the same storage account and put the name here. It will contain all training data.
2. Split the source PDF into individual pages with `scripts/split_pdf.py`. Upload those pages to a folder (preferably with the same name as the original PDF file) in the training container. Learn more [here](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/try-v3-form-recognizer-studio#additional-steps-for-custom-projects).
3. Log-in to FormRecognizerStudio (FRS) and follow this [guide](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/supervised-table-tags) to tag tables. FRS does not have any way to insert or remove arbitrary row so if you make a mistake while tagging, you might have to start from scratch. Luckily we have the script `scripts/edit_fr_table.py` to remove and insert rows. E.g.
   ```bash
   scripts/edit_fr_table.py st-tammany-booking-log-2020/0009.pdf charges insertRow 1 2
   ```
4. Test and provide more training data until the model perform sufficiently well.
5. Extract tables with the custom model using `scripts/extract_tables_from_doc.py`. E.g.
   ```bash
   scripts/extract_tables_from_doc.py https://www.dropbox.com/s/9zmpmhrhtashq2o/st_tammany_booking_log_2020.pdf\?dl\=1 tables/st_tammany_booking_log_2020 --end-page 839 --model-id labeled_11 --batch-size 1
   ```

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

### Run the entire pipeline and commit new data

```bash
make commit_all
```

### Push new data to wrglhub

```bash
make push_all
```

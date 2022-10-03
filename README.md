# LLEAD data integration

Notebooks and Python scripts to combine & integrate LLEAD data. All generated data are kept in [Wrgl](https://hub.wrgl.co/@ipno).

## 1. Getting started

### 1.a. Tools installation

To contribute you must use the following tools:

- **Python 3.9+**: We recommend installing Python 3.9 with [pyenv](https://github.com/pyenv/pyenv) and then creating an env just for this project with [virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/).
- [VSCode](https://code.visualstudio.com/download): This editor has all the features that one could ask for in a Python project. While there might be other great IDEs out there, it is essential that everyone use the same IDE. It makes sharing know-how and collaboration much smoother.
  - [VSCode Python extension](https://marketplace.visualstudio.com/items?itemName=ms-python.python): It highlights code, auto-formats code, and allows you to run Jupyter notebook right inside VSCode.
  - [VSCode Live Share extension](https://marketplace.visualstudio.com/items?itemName=MS-vsliveshare.vsliveshare-pack): It allows you to quickly jump into a call and start a pair-programming session right inside VSCode. Occasional Live Share session is a great way to unstuck a teammate's problem.
- [Wrgl](https://www.wrgl.co/doc/guides/installation): Keep produced CSVs in version control.
- [DVC](https://dvc.org/doc/install): Keep other binary data and model files in version control.

### 1.b. Run all processing steps

```bash
# install all related packages
pip install -r requirements.txt

# pull raw data input with dvc
dvc checkout

# initialize the wrgl repo
wrgl init

# process everything
make

# check whether the output match the schema from data/datavalid.yml
python -m datavalid --dir data
```

## 2. Data integration principles

Every data integration workflow including this one has these steps:

1. **Standardization & cleaning**: The first and most important step. If this produceS inaccurate data, subsequent steps cannot save it.
   - **Data realignment**: If data come from an OCR system such as Textract, this step correctS any column & row misalignment problem. It also corrects any typo introduced by OCR.
   - **Schema matching**: Consolidate columns into predefined schema across all datasets. Condense whitespace, strip and remove unwanted characters and make sure every column names are the same cases.
   - **Data cleaning**: For each column, remove unwanted characters and condense whitespace.
   - **Data standardization**: Make sure values with the same meaning are written the same in every dataset. This applies to both categorical data (such as officer's rank) and more free-form data such as locality, street name, etc.
   - **Data tokenization & segmentation**: This step is needed for columns that hold multiple pieces of information (such as `full_name`, `full_address`) and which doesn't have clearly delineated structure. For each value, a set of possible tags should be applied to the characters, which can then become the basis to segment this column into multiple output columns.
   - **Output verification**: Possible if another source can be consulted. This step ensures the output quality which is important for subsequent steps to also produce high quality output.
2. **Data matching**: Compare data between 2 datasets and see if there are matches. Or if there are duplications in a single dataset this step still need to be carried out as part of the deduplication effort.
   - **Indexing**: If one naively compares each record to every other record then time complexity will become quadratic and make this step incredibly slow. This step selects an index key to divide each dataset into multiple subgroups of data such that similar records are grouped and that there are as few records in each subgroup as possible. Detailed comparisons between records will only happen between records in the same subgroup.
   - **Record comparison**: For every pair of potential matches, each field is compared using a function that produces a number between 0 and 1 (0 denotes total non-match while 1 denotes total match). The output of this step is a numeric comparison vector for every record pair.
   - **Classification**: Based on the comparison vector, each records pairs are classified into: matches, non-matches or potential matches.
3. **Data fusion**: At this stage data can be split or joined to match the output schema. Additional columns may be added if they can be figured out from existing columns (i.e. `middle_initial` from `middle_name`).

## 3. Output schema

This integration pipeline produces the following kind of data for each police agency:

- **Personnel**: Police roster with name and demographic info. They are saved at `data/fuse/per_{agency}.csv`.
- **Allegation**: Charges and complaints against a police officer. They are saved at `data/fuse/com_{agency}.csv`.
- **Event**: Anything with a date except an officer's birth date is counted as an event. See [lib/events.py](lib/events.py) for the full list of events. They are saved at `data/fuse/event_{agency}.csv`.
- **Use of Force**: They are saved at `data/fuse/uof_{agency}.csv`.
- **Stop and Search**: They are saved at `data/fuse/sas_{agency}.csv`.

The last step of the pipeline combines data files from all agencies into one file for each type:

- `data/fuse/personnel.csv`
- `data/fuse/allegation.csv`
- `data/fuse/event.csv`
- `data/fuse/use_of_force.csv`
- `data/fuse/stop_and_search.csv`

See [data/datavalid.yml](data/datavalid.yml) for more details regarding the schema.

## 4. Common tasks & workflows

### 4.a. Adding new datasets

1. **Add raw CSVs**: Add files under a subfolder of `data/raw` folder. Run `scripts/dvc_add.sh` to keep track of them in DVC.
2. **Explore with Jupyter notebook**: We recommend running Jupyter notebooks right within VSCode which is possible if you have the Python extension installed. If you want to save a notebook then please save it in the `notebooks` folder with a distinct name that should at least include the name of the dataset that you were exploring.
3. **Write clean script**: Clean scripts are scripts in the `clean` folder which do what is outlined in the "Standardization & cleaning" step in [data integration principles](#2-data-integration-principles) section. There are some rules for writing clean scripts:
   - Must have a main block which is where the processing begins
   - All input and output must be CSVs
   - Must not accept any argument but rather specify input and output CSVs directly by name via `deba.data`.
   - Must save outputs to the `data/clean` folder using `deba.data`.
   - No dynamically generated CSV name. Otherwise, automated script dependency will not work.
   - Data in a clean script typically pass through multiple steps of processing. Using `pandas.DataFrame.pipe` is the preferred way to join the steps together.
   - When in doubt, consult the existing scripts.
4. **Write match script**: Match scripts are scripts in the `match` folder which do the "Data matching" step in [data integration principles](#2-data-integration-principles) section. We use the [datamatch](https://datamatch.readthedocs.io/en/latest/) library which not only facilitates record linkage but also data deduplication. Datamatch does not use machine learning but relies on a simple threshold-based algorithm. Still, it is very flexible in what it can do and has the added benefits of being easy to understand and running very fast. Match scripts should follow most of the rules for clean scripts with a few additional rules:
   - For each matching task, save the matching result to an Excel file in the `data/match` folder with the name in this format: `{agency}_{source_a}_v_{source_b}.xlsx`. For example, `new_orleans_harbor_pd_cprr_2020_v_pprr_2020.xlsx` shows matched records between `New Orleans Harbor PD CPRR 2020` and `New Orleans Harbor PD PPRR 2020` datasets. See existing match scripts for example.
   - Must save outputs to the `data/match` folder using `deba.data`.
5. **Review matching result**: The previous step should produce one or more Excel files in `data/match` folder showing matched records in an easy-to-review format. Each has 3 sheets:
   - **Sample pairs**: Show a small sample of record pairs for each score range.
   - **All pairs**: Show all pairs of records and their respective score for a more in-depth review. Pairs that score too low (and therefore could never be considered match anyway) are not present.
   - **Decision**: Has 2 values
     - `match_threshold`: the cut-off similarity score that the matcher has decided on. Everything below this score is considered non-match
     - `number_of_matched_pairs`: the number of matched pairs using this threshold.
6. **Write fuse script**: Fuse scripts are scripts in the `fuse` folder which do the "Data fusion" step in [data integration principles](#2-data-integration-principles) section. They follow most of the rules for clean scripts plus a few more rules:
   - Must output one or more of the data files outlined in the [output schema](#3-output-schema) section.
   - Must use functions from `lib.columns` package to validate and rearrange columns for each file type according to the schema in `data/datavalid.yml`.
   - Must save outputs to the `data/fuse` folder using `deba.data`.
7. **Run make**: Literally just run `make`. If there's no problem then you will see new data files being generated.
8. **Check data quality with datavalid**: Run `python -m datavalid --dir data` which will check and print out any error found in the newly generated data.
9. **Add new branches to wrgl config**: Modify the [.wrgl/config.yaml](.wrgl/config.yaml) file to include new data files each as a new branch. Branch name should be the file name with underscores replaced with dashes. E.g. `event_baton_rouge_pd.csv` correspond to branch `event-baton-rouge-pd`.
10. **Pull latest data**: Run `wrgl pull --all`. This pulls all the latest data changes for all branches.
11. **Review your changes**: Run `wrgl diff --all` to see all the changes you made with all existing branches. Run `wrgl diff {branch}` to review detailed changes for a single branch.
12. **Commit new data**: When you are satisfied with the changes, commit new data with `wrgl commit --all "{commit message}"`. The commit message should be short and describe the changes that you made. Something similar to the Git commit message is good.
13. **Push new data**: Run `wrgl push --all`.

### 4.b. Extract tables from PDFs with Form Recognizer

This repository proposes a workflow and some utilty scripts to help extract tables from PDF with [Azure From Recognizer](https://azure.microsoft.com/en-us/services/form-recognizer/). There are 2 workflows:

#### 4.b.i For simple table extraction

You can simply use one of FR's prebuilt models, specifically the [Layout model](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/concept-layout). Just use the web-based FormRecognizerStudio to upload documents and extract data. If there are too many pages to extract manually, we can add the ability to automate extraction from prebuilt models to [scripts/extract_tables_from_doc.py](scripts/extract_tables_from_doc.py).

#### 4.b.ii For complex table extraction

You need to train a custom model and use that model to extract data. Follow these steps:

1. Create a `.env` file (see [python-dotenv](https://pypi.org/project/python-dotenv/) to learn the syntax) at the root directory of this repository with the following environment variable:
   - `FORM_RECOGNIZER_ENDPOINT`: follow [these instruction](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/try-v3-python-sdk#prerequisites) to get the endpoint and key for a Form Recognizer resource.
   - `FORM_RECOGNIZER_KEY`: see above.
   - `BLOB_STORAGE_CONNECTION_STRING`: create an Azure storage account to store training data and follow this [guide](https://docs.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string) to get the connection string.
   - `FORM_RECOGNIZER_CONTAINER`: create a container in the same storage account and put the name here. It will contain all training data.
2. Split the source PDF into individual pages with `scripts/split_pdf.py`. Upload those pages to a folder (preferably with the same name as the original PDF file) in the training container. Learn more [here](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/quickstarts/try-v3-form-recognizer-studio#additional-steps-for-custom-projects).
3. Log in to FormRecognizerStudio (FRS) and follow this [guide](https://docs.microsoft.com/en-us/azure/applied-ai-services/form-recognizer/supervised-table-tags) to tag tables. FRS does not have any way to insert or remove arbitrary rows so if you make a mistake while tagging, you might have to start from scratch. Luckily we have the script `scripts/edit_fr_table.py` to remove and insert rows. E.g.
   ```bash
   scripts/edit_fr_table.py st-tammany-booking-log-2020/0009.pdf charges insertRow 1 2
   ```
4. Test and provide more training data until the model perform sufficiently well.
5. Extract tables with the custom model using [scripts/extract_tables_from_doc.py](scripts/extract_tables_from_doc.py). E.g.
   ```bash
   scripts/extract_tables_from_doc.py https://www.dropbox.com/s/9zmpmhrhtashq2o/st_tammany_booking_log_2020.pdf\?dl\=1 tables/st_tammany_booking_log_2020 --end-page 839 --model-id labeled_11 --batch-size 1
   ```

### 4.c. Working with Wrgl

```bash
# pull all branches
wrgl pull --all

# show changes for all
wrgl diff --all

# show in-depth changes for a single branch
wrgl diff event

# commit a single branch
wrgl commit event "my new commit"

# commit all branches
wrgl commit --all "my new commit"

# log-in with your wrglhub credentials
wrgl credentials authenticate https://hub.wrgl.co/api

# push all changes
wrgl push --all

# push a single branch
wrgl push event
```

## 5. Working with DVC

```bash
# pull all dvc-tracked files
dvc checkout

# authenticate DVC so that you can push new files
gcloud auth login
dvc remote modify --local gcs credentialpath ~/.config/gcloud/legacy_credentials/<your email>/adc.json

# update dvc after making changes
scripts/dvc_add.sh

# push file changes to google cloud storage
dvc push
```

## 6. Automated script dependency

As you might notice, we never have to declare script dependency anywhere because Make can figure out the dependency automatically. We do have to write the scripts in a particular way but the benefits are well worth it. We also use md5 checksums of the scripts as recipe dependencies instead of the scripts themselves, which makes the processing resistant against superfluous file changes caused by Git. See [Makefile](Makefile) and [scripts/write_deps.py](scripts/write_deps.py) to learn more.

## 7. OCR

To keep things consistent, only data files that have had metadata extracted can be fed into an OCR script. That means each OCR script should depend on a single meta script with the same name. E.g. `ocr/minutes.py` depends on `meta/minutes.py`. Furthermore, lib modules used by `meta` and `ocr` stages make many assumptions about data structure and even where the data is stored.

To create a new OCR script, follow these steps:

1. Add a folder containing the raw files under `data` folder
2. DVC add this folder, make sure to point `--file` at a file in the repository root:

   ```bash
   dvc add --file <dvc_file> data/<new_folder>
   # e.g. dvc add --file raw_minutes.dvc data/raw_minutes
   ```

3. Append this command to `scripts/dvc_add.sh` so that we don't need to remember it
4. Create a meta script `meta/<something>.py` with the following content:

   ```python
   from lib.dvc import files_meta_frame
   ...
   if __name__ == "__main__":
      # Always begin by calling files_meta_frame with the dvc file.
      # Doing so will produce a frame that has the correct metadata fields
      # needed for OCR
      df = (
         files_meta_frame("<something>.dvc")
        .pipe(some_other_processing)
      )
      df.to_csv(deba.data("meta/<something>_files.csv"), index=False)
   ```

5. Add a new entry under `overrides` section of `deba.yaml` to add a rule that generates the meta file (Deba does not generate any rule for a meta script because a meta script only takes a DVC file as its input):

   ```yaml
   // deba.yaml
   overrides:
      ...
      - target:
          - meta/<something>_files.csv
        prerequisites:
          - $(DEBA_MD5_DIR)/meta/<something>.py.md5
          - $(DEBA_MD5_DIR)/<something>.dvc.md5
        recipe: "$(call deba_execute,meta/<something>.py)"
   ```

6. Create an OCR script `ocr/<something>.py` with the following content:

   ```python
   from lib.dvc import real_dir_path
   from lib.ocr import process_pdf
   ...
   if __name__ == "__main__":
      # extract raw files directory from the dvc file
      dir_name = real_dir_path("<something>.dvc")
      df = (
         pd.read_csv(deba.data("meta/<something>_files.csv"))
         # assuming all rows are pdf files, if there are other files
         # discard them before feeding into process_pdf
         .pipe(process_pdf, dir_name)
      )
      df.to_csv(deba.data("ocr/<something>_pdfs.csv"), index=False)
   ```

7. Run `make data/ocr/<something>_pdfs.csv` to make sure everything works.
8. Run `scripts/dvc_add.sh && dvc push` to make sure raw files and OCR cache are kept track of and pushed.

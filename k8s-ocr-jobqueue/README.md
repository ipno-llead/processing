# Kubernetes OCR job queue

## Install/update

Check out [pckhoi/k8s-ocr-jobqueue](https://github.com/pckhoi/k8s-ocr-jobqueue)

## Uninstall job queue

```bash
scripts/uninstall_k8s_ocr_jobqueue.sh
```

## OCR workflow

To keep things consistent, only data files that have had metadata extracted can be fed into an OCR script. That means each OCR script should depend on a single meta script with the same name. E.g. `ocr/minutes.py` depends on `meta/minutes.py`. Furthermore, lib modules used by `meta` and `ocr` stages make many assumptions about data structure and even where the data is stored.

To create a new OCR script, follow these steps:

1. Add a folder containing the raw pdf files under `data` folder
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

5. Create an OCR script `ocr/<something>.py` with the following content:

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

6. Run `make data/ocr/<something>_pdfs.csv` to split pdf files into pages and enqueue them onto our OCR job queue.
7. Wait a few days, and run the same command again, using `-B` to force rerun if necessary. This time it will download processed pdf files and report whether they are all processed. Read docstring of `process_pdf` to learn more.
8. Run `scripts/dvc_add.sh && dvc push` to make sure raw files are kept track of and pushed.

## Special environment variables

During OCR script execution, there are a few environment variables that can control how it behaves:

| Environment variable     | Description                                                                        |
| ------------------------ | ---------------------------------------------------------------------------------- |
| OCR_REQUEUE              | Requeue all pdf files regardless of whether they were previously processed or not. |
| OCR_REQUEUE_UNSUCCESSFUL | Requeue any pdf files that were not queued or were not processed completely.       |
| OCR_ENSURE_COMPLETE      | Raise an error unless all pdf files were processed successfully.                   |

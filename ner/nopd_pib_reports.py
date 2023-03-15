import spacy
import deba
import pandas as pd
from lib.ner import train_spacy_model, apply_spacy_model


def read_pdfs():
    pdfs = pd.read_csv(deba.data("ocr/nopd_pib_reports_pdfs_2014_2020.csv"))
    return pdfs


def training_data():
    return deba.data("data/raw/new_orleans_pd/training_data/nopd_pib_reports.jsonl")


if __name__ == "__main__":
    read_training_data = training_data()
    pdfs = read_pdfs()
    # model = train_spacy_model(pdfs, read_training_data)
    # model = model.to_disk(deba.data("raw/new_orleans_pd/model/nopd_pib_reports.model"))

    load_model = spacy.load(
        "data/raw/new_orleans_pd/model/nopd_pib_reports.model")
    ner = apply_spacy_model(pdfs, load_model)
    ner.to_csv(deba.data("ner/nopd_pib_reports_2014_2020.csv"), index=False)

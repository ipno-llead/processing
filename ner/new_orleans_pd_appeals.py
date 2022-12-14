import spacy
import deba
import pandas as pd
from lib.ner import train_spacy_model, apply_spacy_model


def read_pdfs():
    pdfs = pd.read_csv(deba.data("ocr/new_orleans_pd_appeals_pdfs.csv"))
    return pdfs


def training_data():
    training_data = (
        r"data/raw/new_orleans_pd/training_data/new_orleans_pd_appeals.jsonl"
    )
    return training_data


if __name__ == "__main__":
    read_training_data = training_data()
    pdfs = read_pdfs()
    # ner = train_spacy_model(pdfs, read_training_data)
    # model = ner.to_disk(
    #     deba.data("raw/new_orleans_pd/model/new_orleans_pd_appeals_v2.model")
    # )
    load_model = spacy.load(
        deba.data("raw/new_orleans_pd/model/new_orleans_pd_appeals.model")
    )
    ner = apply_spacy_model(pdfs, load_model)
    ner.to_csv(deba.data("ner/new_orleans_pd_appeals_pdfs.csv"), index=False)

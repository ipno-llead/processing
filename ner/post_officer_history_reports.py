import json
import random
import spacy
from spacy.util import minibatch, compounding
from spacy.training import Example
import pandas as pd
import deba
import pandas as pd
from lib.ner import train_spacy_model, apply_spacy_model


def read_pdfs_21():
    pdfs = pd.read_csv(deba.data("ocr/post_officer_history_reports_pdfs.csv"))
    return pdfs


def read_pdfs_22():
    pdfs = pd.read_csv(deba.data("ocr/post_officer_history_reports_9_16_2022_pdfs.csv"))
    return pdfs


def training_data():
    data = (
        r"data/raw/post/post_officer_history/training_data/post_officer_history.jsonl"
    )
    return data


if __name__ == "__main__":
    pdfs_21 = read_pdfs_21()
    pdfs_22 = read_pdfs_22()
    training = training_data()
    # ner = train_spacy_model(pdfs, training)
    # trained_model = spacy.load("data/ner/post/post_officer_history/model/post_officer_history.model")
    trained_model = spacy.load(
        "data/ner/post/post_officer_history/model/post_officer_history.model"
    )
    ner21 = apply_spacy_model(pdfs_21, trained_model)
    ner22 = apply_spacy_model(pdfs_22, trained_model)
    ner21.to_csv(deba.data("ner/post_officer_history_reports.csv"), index=False)
    ner22.to_csv(deba.data("ner/post_officer_history_reports_9_16_2022.csv"), index=False)

import json
import random
import spacy
from spacy.util import minibatch, compounding
from spacy.training import Example
import pandas as pd
import deba
import pandas as pd
from lib.ner import train_spacy_model, apply_spacy_model


def read_pdfs():
    pdfs = pd.read_csv(deba.data("ocr/post_officer_history_reports_pdfs.csv"))
    return pdfs


def training_data():
    data = (
        r"data/raw/post/post_officer_history/training_data/post_officer_history.jsonl"
    )
    return data


if __name__ == "__main__":
    pdfs = read_pdfs()
    training = training_data()
    # ner = train_spacy_model(pdfs, training)
    # trained_model = spacy.load("data/ner/post/post_officer_history/model/post_officer_history.model")
    trained_model = spacy.load(
        "data/ner/post/post_officer_history/model/post_officer_history.model"
    )
    ner = apply_spacy_model(pdfs, trained_model)
    ner.to_csv(deba.data("ner/post_officer_history_reports.csv"), index=False)

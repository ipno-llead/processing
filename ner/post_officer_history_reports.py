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


def read_pdfs_advocate():
    pdfs = pd.read_csv(deba.data("ocr/post_officer_history_reports_advocate_pdfs.csv"))
    return pdfs


def training_data():
    data = (
        r"data/raw/post/post_officer_history/training_data/post_officer_history_training_data.jsonl"
    )
    return data


if __name__ == "__main__":
    pdfs_21 = read_pdfs_21()
    pdfs_22 = read_pdfs_22()
    pdfs_advocate = read_pdfs_advocate()
    training = training_data()
    ner = train_spacy_model(pdfs_21, training)
    model = ner.to_disk(deba.data("ner/post/post_officer_history/model/post_officer_history_2.model"))
    # trained_model = spacy.load(
    #     "data/ner/post/post_officer_history/model/post_officer_history.model"
    # )
    ner_21 = apply_spacy_model(pdfs_21, ner)
    ner_22 = apply_spacy_model(pdfs_22, ner)
    ner_advocate = apply_spacy_model(pdfs_advocate, ner)
    ner_21.to_csv(deba.data("ner/post_officer_history_reports.csv"), index=False)
    ner_22.to_csv(deba.data("ner/post_officer_history_reports_2022.csv"), index=False)
    ner_advocate.to_csv(deba.data("ner/advocate_post_officer_history_reports.csv"), index=False)

    
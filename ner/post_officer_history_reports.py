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

def read_pdfs_22_rotated():
    pdfs = pd.read_csv(deba.data("ocr/post_officer_history_reports_9_30_2022_rotated_pdfs.csv"))
    return pdfs


def read_pdfs_23():
    pdfs = pd.read_csv(
        deba.data("ocr/post_officer_history_reports_2023_rotated_pdfs.csv")
    )
    return pdfs


def read_pdfs_advocate():
    pdfs = pd.read_csv(deba.data("ocr/post_officer_history_reports_advocate_pdfs.csv"))
    return pdfs


def training_data():
    return deba.data("raw/post/post_officer_history/training_data/post_ohr_training_data.jsonl")


if __name__ == "__main__":
    pdfs_21 = read_pdfs_21()
    pdfs_22 = read_pdfs_22()
    pdfs_advocate = read_pdfs_advocate()
    pdfs_23 = read_pdfs_23()
    pdfs_22_rotated = read_pdfs_22_rotated()
    training = training_data()
    # ner = train_spacy_model(pdfs_21, training)
    # model = ner.to_disk(
    #     deba.data("ner/post/post_officer_history/model/post_officer_history_4.model")
    # )
    trained_model = spacy.load(deba.data(
        ("ner/post/post_officer_history/model/post_officer_history.model")
    ))
    ner_21 = apply_spacy_model(pdfs_21, trained_model)
    ner_22 = apply_spacy_model(pdfs_22, trained_model)
    ner_22_rotated = apply_spacy_model(pdfs_22_rotated, trained_model)
    ner_23 = apply_spacy_model(pdfs_23, trained_model)
    ner_advocate = apply_spacy_model(pdfs_advocate, trained_model)
    ner_21.to_csv(deba.data("ner/post_officer_history_reports.csv"), index=False)
    ner_22.to_csv(deba.data("ner/post_officer_history_reports_2022.csv"), index=False)
    ner_22_rotated.to_csv(deba.data("ner/post_officer_history_reports_2022_rotated.csv"), index=False)
    ner_23.to_csv(deba.data("ner/post_officer_history_reports_2023.csv"), index=False)
    ner_advocate.to_csv(
        deba.data("ner/advocate_post_officer_history_reports.csv"), index=False
    )

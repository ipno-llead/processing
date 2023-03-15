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
    pdfs = pd.read_csv(deba.data("ocr/reports_louisiana_state_pd_2020_pdfs.csv"))
    return pdfs


def training_data():
    return deba.data("data/raw/louisiana_state_pd/training_data/louisiana_state_pd_reports_2020.jsonl")


if __name__ == "__main__":  
    pdfs = read_pdfs()
    # training = training_data()
    # ner = train_spacy_model(pdfs, training)
    # ner.to_disk(
    #     deba.data("ner/louisiana_state_pd/model/reports_louisiana_state_pd_2020.model")
    # )
    trained_model = spacy.load(
        "data/ner/louisiana_state_pd/model/reports_louisiana_state_pd_2020.model"
    )
    ner = apply_spacy_model(pdfs, trained_model)
    ner.to_csv(deba.data("ner/reports_louisiana_state_pd_2020.csv"), index=False)

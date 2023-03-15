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
    pdfs = pd.read_csv(deba.data("ocr/letters_louisiana_state_pd_2019_pdfs.csv"))
    return pdfs


def training_data():
    return deba.data("data/raw/louisiana_state_pd/training_data/louisiana_state_pd_letters_2019.jsonl")


if __name__ == "__main__":
    pdfs = read_pdfs()
    training = training_data()
    # ner = train_spacy_model(pdfs, training)
    # ner.to_disk(
    #     deba.data("ner/louisiana_state_pd/model/letters_louisiana_state_pd_2019.model")
    # )
    trained_model = spacy.load(
        "data/ner/louisiana_state_pd/model/letters_louisiana_state_pd_2019.model"
    )
    ner = apply_spacy_model(pdfs, trained_model)
    ner.to_csv(deba.data("ner/letters_louisiana_state_pd_2019.csv"), index=False)

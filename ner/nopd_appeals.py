import json
import random
import spacy
from spacy.util import minibatch, compounding
from spacy.training import Example
import pandas as pd
import deba
import pandas as pd

from lib.ner import train_spacy_model_for_dict_format, apply_spacy_model


def read_pdfs():
    pdfs = pd.read_csv(deba.data("ocr/nopd_appeals_pdfs.csv"))
    return pdfs


def training_data():
    training_data = r"data/raw/new_orleans_pd/training_data/ayyub.jsonl"
    return training_data


if __name__ == "__main__":
    pdfs = read_pdfs()
    read_training_data = training_data()
    # model = train_spacy_model_for_dict_format(pdfs, read_training_data)
    # model = model.to_disk(deba.data("raw/new_orleans_pd/model/leia.model"))

    load_model = spacy.load(deba.data("raw/new_orleans_pd/model/leia.model"))
    ner = apply_spacy_model(pdfs, load_model)
    ner.to_csv(deba.data("ner/nopd_appeals_pdfs.csv"), index=False)

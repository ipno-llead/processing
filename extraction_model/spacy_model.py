import json
import random
import spacy
from spacy.util import minibatch, compounding, decaying
from spacy.training import example
import pandas as pd
import os
import deba


def spacy_model():
    #  import labeled data that has been exported from doccano
    labeled_data = []
    with open(deba.data("raw/post/json/ayyub.jsonl")) as read_file:
        for line in read_file:
            data = json.loads(line)
            labeled_data.append(data)

    TRAINING_DATA = []
    for entry in labeled_data:
        entities = []
        for e in entry["label"]:
            entities.append((e[0], e[1], e[2]))
        spacy_entry = (entry["data"], {"entities": entities})
        TRAINING_DATA.append(spacy_entry)

    nlp = spacy.blank("en")
    ner = nlp.create_pipe("ner")
    nlp.add_pipe("ner")
    ner.add_label("officer_name")
    ner.add_label("officer_sex")
    ner.add_label("agency")

    optimizer = nlp.begin_training()
    for itn in range(500):
        random.shuffle(TRAINING_DATA)
        losses = {}
        for batch in spacy.util.minibatch(
            TRAINING_DATA, size=compounding(2.0, 32.0, 1.001)
        ):
            for text, annotations in batch:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                nlp.update([example], sgd=optimizer, losses=losses, drop=0.2)
        print(losses)

    nlp.to_disk(deba.data("raw/post/json/padme.model"))

    directory = os.listdir(deba.data("raw/post/ocr"))
    os.chdir(deba.data("raw/post/ocr"))

    #  run Spacy model
    for file in directory:
        if file.endswith(".txt"):
            open_file = open(file, "r")
            read_file = open_file.read()

            ents = []
            docs = nlp(read_file)
            entities = [ents.text for ents in docs.ents]
            ents.append(entities)

            df = pd.DataFrame(ents, columns=[ents.label_ for ents in docs.ents])
            file_name = file + ".csv"
            path = deba.data("raw/post/spacy/")
            df.to_csv(path + file_name, index=False)


if __name__ == "__main__":
    df = spacy_model()

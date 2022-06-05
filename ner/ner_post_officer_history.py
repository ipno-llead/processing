import json
import random
import spacy
from spacy.util import minibatch, compounding
from spacy.training import Example
import pandas as pd
import zipfile
import os


### unzip folder containing pre-trained model downloaded from DropBox
# with zipfile.ZipFile("data/ner/post/post_officer_history/model/padme.model.zip", 'r') as zip_ref:
#     zip_ref.extractall("data/ner/post/post_officer_history/model/padme.model")


def spacy_model():
    ###  import labeled data that has been exported from doccano

    labeled_data = []
    with open(
        r"data/ner/post/post_officer_history/training_data/ayyub.jsonl", "rb"
    ) as read_file:
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

    ### load trained model: nlp = spacy.load("padme.model") or .
    ### train model

    # nlp = spacy.load("data/ner/post/post_officer_history/model/padme.model")
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
            TRAINING_DATA, size=compounding(3.0, 2.0, 1.001)
        ):
            for text, annotations in batch:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                nlp.update([example], sgd=optimizer, losses=losses, drop=0.4)
        print(losses)

    # save model to disk:
    # nlp.to_disk("data/ner/post/post_officer_history/model/padmes.model")
    dir_path = os.chdir("data/ocr/post/post_officer_history/output")

    ###  apply model to data
    for file in os.listdir(dir_path):
        if file.endswith(".txt"):
            open_file = open(file, "r")
            read_file = open_file.read()

            ents = []
            docs = nlp(read_file)
            entities = [ents.text for ents in docs.ents]
            ents.append(entities)

            df = pd.DataFrame(ents, columns=[ents.label_ for ents in docs.ents])
            file_name = file + ".csv"
            path = r"../../../../ner/post/post_officer_history/output/"
            df.to_csv(path + file_name, index=False)
    return df


if __name__ == "__main__":
    ner = spacy_model()

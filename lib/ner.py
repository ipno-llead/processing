import json
import random
import spacy
from spacy.util import minibatch, compounding
from spacy.training import Example
import pandas as pd
import deba


def train_spacy_model(df: pd.DataFrame, training_data: str) -> pd.DataFrame:
    ###  import labeled_data that has been exported from doccano

    labeled_data = []
    with open(
        training_data,
        "r",
    ) as read_file:
        for line in read_file:
            data = json.loads(line)
            labeled_data.append(data)

    TRAINING_DATA = []
    for entry in labeled_data:
        entities = []
        for e in entry["label"]:
            entities.append((e[0], e[1], e[2]))
        spacy_entry = (entry["text"], {"entities": entities})
        TRAINING_DATA.append(spacy_entry)

    ### train model
    nlp = spacy.blank("en")
    ner = nlp.create_pipe("ner")
    nlp.add_pipe("ner")
    ner.add_label("officer_name")
    ner.add_label("officer_sex")
    ner.add_label("agency")

    optimizer = nlp.begin_training()
    for itn in range(50):
        random.shuffle(TRAINING_DATA)
        losses = {}
        for batch in spacy.util.minibatch(
            TRAINING_DATA, size=compounding(5.0, 2.0, 1.001)
        ):
            for text, annotations in batch:
                doc = nlp.make_doc(text)
                example = Example.from_dict(doc, annotations)
                nlp.update([example], sgd=optimizer, losses=losses, drop=0.35)
                print(losses)
        entities = []
    return nlp


def apply_spacy_model(df: pd.DataFrame, spacy_model: str) -> pd.DataFrame:
    nlp = spacy_model
    ### train model
    entities = []
    for row in df["paragraphs"].apply(nlp):
        text = [text.text for text in row.ents]
        labels = [labels.label_ for labels in row.ents]
        ents = list(zip(labels, text))

        tuples = [i[0] for i in ents]
        counts = {key: tuples.count(key) for key in [i[0] for i in ents]}
        for idx, key in enumerate(tuples):
            if counts.get(key) and counts.get(key) > 1:
                for num in range(counts[key]):
                    if key + str(num + 1) not in tuples:
                        tuples.remove(key)
                        tuples.insert(idx + num, key + "_" + str(num + 1))

        renamed_ents = dict(zip(tuples, [i[1] for i in ents]))
        entities.append(renamed_ents)

    ner = pd.DataFrame(entities)
    df = pd.concat([ner, df], axis=1)
    return df

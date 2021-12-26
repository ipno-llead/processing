import sys

sys.path.append("../")
import json
import random
import spacy
import pandas as pd
import os
from lib.path import data_file_path

# #  import labeled data that has been exported from doccano
labeled_data = []

with open(
    data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs/training_data/ayyub.jsonl")
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


#  load a spacy model OR
# nlp = spacy.load("C:/Users/ayyubi/extraction/data/raw/new_orleans_csc/training/json/nopd.model")

# #  train a new spacy model
nlp = spacy.blank("en")
ner = nlp.create_pipe("ner")
nlp.add_pipe(ner)
ner.add_label("docket_number")
ner.add_label("accused_name")
ner.add_label("incident_summary")
ner.add_label("appeal_disposition")
ner.add_label("allegation")
ner.add_label("initial_action")
ner.add_label("notification_of_action_date")
ner.add_label("incident_date")
ner.add_label("nopd_incident_report_number")
ner.add_label("attorney_name")
ner.add_label("appeal_date")

nlp.begin_training()
for itn in range(25):
    random.shuffle(TRAINING_DATA)
    losses = {}
    for batch in spacy.util.minibatch(TRAINING_DATA, size=2):
        texts = [text for text, entities in batch]
        annotations = [entities for text, entities in batch]
        nlp.update(texts, annotations, losses=losses, drop=0.3)
    print(losses)


# #  save a model
nlp.to_disk(
    data_file_path(
        "raw/new_orleans_csc/appeal_hearing_pdfs/nopd_appeals_spacy_model.12_25_25.model"
    )
)

directory = os.listdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))
os.chdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))

#  run Spacy model on test data

for file in directory:
    if file.endswith(".txt"):
        ents = []
        raw_text = []

        open_file = open(file, "r")
        read_file = open_file.read()
        docs = nlp(read_file)

        entities = [ents.text for ents in docs.ents]
        ents.append(entities)
        dfa = pd.DataFrame(ents, columns=[ents.label_ for ents in docs.ents])

        transcripts = [(" ".join(token.text for token in docs))]
        raw_text.append(transcripts)
        dfb = pd.DataFrame(raw_text, columns=["raw_rext"])

        dfa["tmp"] = "1"
        dfb["tmp"] = "1"

        df = pd.merge(dfa, dfb, on=["tmp"], how="outer")
        df = df.drop(columns=["tmp"])
        df["agency"] = "New Orleans PD"

        file_name = "extracted_" + file + ".csv"
        df.to_csv(
            data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs/" + file_name),
            index=False,
        )

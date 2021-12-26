import sys

sys.path.append("../")
import os
import nltk
from nltk.tokenize import punkt
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from lib.path import data_file_path


directory = os.listdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))
os.chdir(data_file_path("raw/new_orleans_csc/appeal_hearing_pdfs"))


for file in directory:
    if file.endswith(".txt"):
        open_file = open(file, "r", encoding="utf-8")
        read_file = open_file.read()

        # tokenize
        token_list = nltk.word_tokenize(read_file)

        # Conversion of all words to lower case
        token_list = [word.lower() for word in token_list]

        # remove punctuation
        token_list = list(
            filter(lambda token: punkt.PunktToken(token).is_non_punct, token_list)
        )

        # remove stopwords
        token_list = list(
            filter(lambda token: token not in stopwords.words("english"), token_list)
        )

        # I want to isolate lemmatization to text_preprocessing for when I'm training a model
        # original text should be included in output for documents table, I think?
        lemmatizer = WordNetLemmatizer()
        token_list = [lemmatizer.lemmatize(word) for word in token_list]

        output = " ".join([lemmatizer.lemmatize(w) for w in token_list])
        # print(output[0:20],"\n")
        # print("Total tokens : ", len(output))

        paragraphs = []
        paragraphs.append(output)

        write_file = open(file, "w", encoding="utf-8")
        write_file.write(str(paragraphs))

import pandas as pd
from top2vec import Top2Vec

def bert_vectors():
    bert_train_vecs = pd.read_csv("vectors/bert_train_vecs.csv")
    bert_train_vecs = bert_train_vecs.values.tolist()

    bert_test_vecs = pd.read_csv("vectors/bert_test_vecs.csv")
    bert_test_vecs = bert_test_vecs.values.tolist()
    return bert_train_vecs, bert_test_vecs


def gensim_vectors():
    gensim_train_vecs = pd.read_csv("vectors/gensim_train_vecs.csv")
    gensim_train_vecs = gensim_train_vecs.values.tolist()

    gensim_test_vecs = pd.read_csv("vectors/gensim_test_vecs.csv")
    gensim_test_vecs = gensim_test_vecs.values.tolist()
    return gensim_train_vecs, gensim_test_vecs


def top2vec_vectors():
    t2v_train_model = Top2Vec.load("models/top2vec_train_model")
    t2v_train_vecs = t2v_train_model.document_vectors

    t2v_test_vecs = pd.read_csv("vectors/t2v_test_vecs.csv")
    t2v_test_vecs = t2v_test_vecs.values.tolist()
    return t2v_train_vecs, t2v_test_vecs
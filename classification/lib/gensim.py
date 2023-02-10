import spacy
import gensim
import gensim.corpora as corpora 
from gensim.models import TfidfModel


def generate_corpus(docs):
    def lemmatization(docs, allowed_pos_tags=["NOUN", "ADJ", "VERB", "ADV"]):
        nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
        final_text = []
        for desc in docs:
            doc = nlp(desc)
            new_text = " ".join([token.lemma_ for token in doc if token.pos_ in allowed_pos_tags])
            final_text.append(new_text)
        return (final_text)
    
    lemmatized_texts = lemmatization(docs)

    def gen_words(lemmatized_texts):
        final = []
        for text in lemmatized_texts:
            new = gensim.utils.simple_preprocess(text, deacc=True)
            final.append(new)
        return (final)
    
    words = gen_words(lemmatized_texts)
    bigram_phrases = gensim.models.Phrases(words, min_count=5, threshold=50)
    trigram_phrases = gensim.models.Phrases(bigram_phrases[words], threshold=50)

    bigram = gensim.models.phrases.Phraser(bigram_phrases)
    trigram = gensim.models.phrases.Phraser(trigram_phrases)

        
    def make_bigrams(texts):
        return list(bigram[doc] for doc in texts)

    def make_trigrams(texts):
        return list(trigram[bigram[doc]] for doc in texts)

    data_bigrams = make_bigrams(words)
    data_bigrams_trigrams = make_trigrams(words)

    id2word = corpora.Dictionary(data_bigrams_trigrams)

    texts = data_bigrams_trigrams

    train_corpus = [id2word.doc2bow(text) for text in texts]

    tdidf = TfidfModel(train_corpus, id2word=id2word)

    low_value = 0.03
    words = []
    words_missing_in_tdif = []

    for i in range(0, len(train_corpus)):
        bow = train_corpus[i]
        low_value_words = []
        tdif_ids = [id for id, value in tdidf[bow]]
        bow_ids = [id for id, value in bow]
        low_value_words = [id for id, value in tdidf[bow] if value < low_value]
        drops = low_value_words+words_missing_in_tdif
        for item in drops:
            words.append(id2word[item])
        words_missing_in_tdif = [id for id in bow_ids if id not in tdif_ids]

        new_bow = [b for b in bow if b[0] not in low_value_words and b[0] not in words_missing_in_tdif]
        train_corpus[i] = new_bow
    return train_corpus, id2word


def generate_gensim_vecs(docs, gensim_model, train_corpus):
    gensim_train_vecs = []
    for i in range(len(docs)):
        top_topics = gensim_model.get_document_topics(train_corpus[i], minimum_probability=0.0)
        topic_vec = [top_topics[i][1] for i in range(20)]
        topic_vec.extend([len(docs.iloc[i])]) 
        gensim_train_vecs.append(topic_vec)
    return gensim_train_vecs
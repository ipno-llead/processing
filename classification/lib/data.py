import pandas as pd


def training_data():
    training_data = pd.read_csv("../data/classification/training_data/training_data.csv")
    return training_data

def test_data():
    test_data = pd.read_csv("../data/classification/test_data/test_data.csv")
    return test_data
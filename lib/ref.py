import os


FIRST_NAMES = []
with open(os.path.join(os.path.dirname(__file__), "../references/us_firstname.txt"), 'r') as f:
    s = f.read()
    FIRST_NAMES = s.strip().split("\n")

LAST_NAMES = []
with open(os.path.join(os.path.dirname(__file__), "../references/us_surname.txt"), 'r') as f:
    s = f.read()
    LAST_NAMES = s.strip().split("\n")


def is_firstname(s):
    return s in FIRST_NAMES


def is_lastname(s):
    return s in LAST_NAMES

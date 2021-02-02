import pandas as pd
from .classifiers import MatchStatus


def match_records(index, classifier, dfa, dfb):
    """
    Group records into different blocks using given index before classifying into matches and potential matches
    """
    if dfa.index.duplicated().any() or dfb.index.duplicated().any():
        raise ValueError(
            "Dataframe index contains duplicates. Both dateframes need to have index free of duplicates.")
    index.index(dfa, dfb)

    matches = []
    non_matches = []
    potential_matches = []
    for rows_a, rows_b in index:
        for idx_a, ser_a in rows_a.iterrows():
            for idx_b, ser_b in rows_b.iterrows():
                st = classifier.classify(ser_a, ser_b)
                if st == MatchStatus.YES:
                    matches.append((idx_a, idx_b))
                elif st == MatchStatus.MAYBE:
                    potential_matches.append((idx_a, idx_b))
                else:
                    non_matches.append((idx_a, idx_b))
    return matches, potential_matches, non_matches


def print_match_result(dfa, dfb, matches, potential_matches, non_matches):
    if len(matches) > 0:
        print("*** MATCHES ***")
        for idx_a, idx_b in matches:
            print(pd.DataFrame([dfa.loc[idx_a], dfb.loc[idx_b]]))
            print()
    if len(potential_matches) > 0:
        print("*** POTENTIAL MATCHES ***")
        for idx_a, idx_b in potential_matches:
            print(pd.DataFrame([dfa.loc[idx_a], dfb.loc[idx_b]]))
            print()
    if len(non_matches) > 0:
        print("*** NON-MATCHES ***")
        for idx_a, idx_b in non_matches:
            print(pd.DataFrame([dfa.loc[idx_a], dfb.loc[idx_b]]))
            print()

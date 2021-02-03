import pandas as pd
from .classifiers import MatchStatus


def match_records(index, classifier, dfa, dfb):
    """
    Group records into different blocks using given index before classifying into matches and potential matches
    """
    if dfa.index.duplicated().any() or dfb.index.duplicated().any():
        raise ValueError(
            "Dataframe index contains duplicates. Both frames need to have index free of duplicates.")
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
    print("matches: %d\npotential matches: %d\nnon-matches: %d" % (
        len(matches), len(potential_matches), len(non_matches)
    ))
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)
    if len(matches) > 0:
        print("\n*** MATCHES ***\n")
        for idx_a, idx_b in matches:
            print(pd.DataFrame([dfa.loc[idx_a], dfb.loc[idx_b]]))
            print()
    if len(potential_matches) > 0:
        print("\n*** POTENTIAL MATCHES ***\n")
        for idx_a, idx_b in potential_matches:
            print(pd.DataFrame([dfa.loc[idx_a], dfb.loc[idx_b]]))
            print()
    if len(non_matches) > 0:
        print("\n*** NON-MATCHES ***\n")
        for idx_a, idx_b in non_matches:
            print(pd.DataFrame([dfa.loc[idx_a], dfb.loc[idx_b]]))
            print()

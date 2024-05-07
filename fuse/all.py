import pandas as pd
import deba
from datamatch import JaroWinklerSimilarity, ThresholdMatcher, ColumnsIndex
from lib.uid import gen_uid
import numpy as np

def extract_names(title):
    title = title.replace('Appeal hearing: ', '').split(' on ')[0].rstrip('.')
    if title.startswith('Officer '):
        title = title[len('Officer '):]
    
    name_parts = title.split(',')[0].split()
    
    if not name_parts:
        return None, None, None
    
    if len(name_parts) == 2:
        first_name, last_name = name_parts
        middle_name = None
    elif len(name_parts) == 3:
        first_name, middle_name, last_name = name_parts
    else:
        first_name = name_parts[0]
        last_name = name_parts[-1]
        middle_name = ' '.join(name_parts[1:-1])
    
    return first_name, middle_name, last_name

def match_docs_with_personnel(documents, per):
    documents = documents.pipe(gen_uid, ["first_name", "middle_name", "last_name", "agency"])
    dfa = (
        documents.loc[documents.uid.notna(), ["uid", "first_name", "middle_name", "last_name", "agency"]]
        .drop_duplicates(subset=["uid"])
        .set_index("uid", drop=True)
    )
    dfa.loc[:, "fc"] = dfa.first_name.fillna("").map(lambda x: x[:1])
    dfb = (
        per[["uid", "first_name", "middle_name", "last_name", "agency"]]
        .drop_duplicates()
        .set_index("uid", drop=True)
    )
    dfb.loc[:, "fc"] = dfb.first_name.fillna("").map(lambda x: x[:1])
    matcher = ThresholdMatcher(
        ColumnsIndex(["fc"]),
        {
            "first_name": JaroWinklerSimilarity(),
            "middle_name": JaroWinklerSimilarity(),
            "last_name": JaroWinklerSimilarity(),
            "agency": JaroWinklerSimilarity(),
        },
        dfa,
        dfb,
    )
    decision = .70
    matcher.save_pairs_to_excel(
        deba.data("fuse/documents_to_per.xlsx"),
        decision,
    )
    matches = matcher.get_index_pairs_within_thresholds(decision)
    match_dict = dict(matches)
    
    documents["matched_uid"] = documents["uid"].map(lambda x: match_dict.get(x, np.nan))
    
    uid_map = documents[documents["matched_uid"].notna()].set_index("uid")["matched_uid"].to_dict()
    
    documents["matched_uid"] = documents["uid"].map(uid_map).fillna(documents["matched_uid"])
    return documents

if __name__ == "__main__":
    uof_df = pd.read_csv(deba.data("fuse_agency/use_of_force.csv"))
    sas_df = pd.read_csv(deba.data("fuse_agency/stop_and_search.csv"))
    appeals_df = pd.read_csv(deba.data("fuse_agency/appeals.csv"))
    allegation_df = pd.read_csv(deba.data("fuse_agency/allegation.csv"))
    brady_df = pd.read_csv(deba.data("fuse_agency/brady.csv"))
    citizens = pd.read_csv(deba.data("fuse_agency/citizens.csv"))
    agencies = pd.read_csv(deba.data("fuse_agency/agency_reference_list.csv"))
    documents = pd.read_csv(deba.data("clean/documents.csv"))
    post = pd.read_csv(deba.data("match_history/post_officer_history.csv"))
    event_df = pd.read_csv(deba.data("match_history/event.csv"))
    per_df = pd.read_csv(deba.data("match_history/personnel.csv"))
    coaccusals = pd.read_csv(deba.data("analysis/coaccusals.csv"))
    cprr_advocate = pd.read_csv(deba.data("match_history/com_the_advocate.csv"))
    per_advocate = pd.read_csv(deba.data("match_history/per_the_advocate.csv"))
    event_advocate = pd.read_csv(deba.data("match_history/event_the_advocate.csv"))

    per_df = pd.concat([per_df, per_advocate], axis=0)
    per_df = per_df.drop_duplicates(subset=["uid"])
    allegation_df = pd.concat([allegation_df, cprr_advocate], axis=0)
    allegation_df = allegation_df.drop_duplicates(subset=["uid", "allegation_uid"])
    event_df = pd.concat([event_df, event_advocate], axis=0)
    event_df = event_df.drop_duplicates(subset=["event_uid"])
    brady_df = brady_df.drop_duplicates(subset=["brady_uid"])

    coaccusals["coaccusal"] = "True"
    coaccusals = coaccusals[["allegation_uid", "coaccusal"]]

    allegation_df = pd.merge(
        allegation_df, coaccusals, on="allegation_uid", how="outer"
    )
    allegation_df.loc[:, "coaccusal"] = allegation_df.coaccusal.fillna("").str.replace(
        r"^$", "False", regex=True
    )

    event_df = event_df[event_df["uid"].isin(per_df["uid"])]
    uof_df = uof_df[uof_df["uid"].isin(per_df["uid"])]
    sas_df = sas_df[sas_df["uid"].isin(per_df["uid"])]
    appeals_df = appeals_df[appeals_df["uid"].isin(per_df["uid"])]
    allegation_df = allegation_df[allegation_df["uid"].isin(per_df["uid"])]
    brady_df = brady_df[brady_df["uid"].isin(per_df["uid"])]

    documents[['first_name', 'middle_name', 'last_name']] = documents['title'].apply(lambda x: pd.Series(extract_names(x)))
    documents = match_docs_with_personnel(documents, per_df)

    documents = documents.drop(columns=['first_name', 'middle_name', 'last_name'])

    uof_df.to_csv(deba.data("fuse/use_of_force.csv"), index=False)
    sas_df.to_csv(deba.data("fuse/stop_and_search.csv"), index=False)
    appeals_df.to_csv(deba.data("fuse/appeals.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse/allegation.csv"), index=False)
    brady_df.to_csv(deba.data("fuse/brady.csv"), index=False)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
    citizens.to_csv(deba.data("fuse/citizens.csv"), index=False)
    agencies.to_csv(deba.data("fuse/agency_reference_list.csv"), index=False)
    documents.to_csv(deba.data("fuse/documents.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event.csv"), index=False)
    per_df.to_csv(deba.data("fuse/personnel.csv"), index=False)

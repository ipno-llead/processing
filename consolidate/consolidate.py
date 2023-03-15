import pandas as pd
import deba


if __name__ == "__main__":
    per_df = pd.read_csv(deba.data("fuse/personnel.csv"))
    allegation_df = pd.read_csv(deba.data("fuse/allegation.csv"))
    uof_df = pd.read_csv(deba.data("fuse/use_of_force.csv"))
    event_df = pd.read_csv(deba.data("fuse/event.csv"))
    sas_df = pd.read_csv(deba.data("fuse/stop_and_search.csv"))
    appeals_df = pd.read_csv(deba.data("fuse/appeals.csv"))
    person_df = pd.read_csv(deba.data("fuse/person.csv"))
    
    allegation_df = allegation_df[allegation_df["uid"].isin(per_df["uid"])]
    uof_df = uof_df[uof_df["uid"].isin(per_df["uid"])]
    event_df = event_df[event_df["uid"].isin(per_df["uid"])]
    sas_df = sas_df[sas_df["uid"].isin(per_df["uid"])]
    appeals_df = appeals_df[appeals_df["uid"].isin(per_df["uid"])]
    person_df = person_df[person_df["canonical_uid"].isin(per_df["uid"])]



    event_df = event_df[~((event_df.agency.fillna("") == ""))]

    per_df.to_csv(deba.data("consolidate/personnel.csv"), index=False)
    event_df.to_csv(deba.data("consolidate/event.csv"), index=False)
    uof_df.to_csv(deba.data("consolidate/use_of_force.csv"), index=False)
    event_df.to_csv(deba.data("consolidate/event.csv"), index=False)
    sas_df.to_csv(deba.data("consolidate/stop_and_search.csv"), index=False)
    appeals_df.to_csv(deba.data("consolidate/appeals.csv"), index=False)
    allegation_df.to_csv(deba.data("consolidate/allegation.csv"), index=False)
    person_df.to_csv(deba.data("consolidate/person.csv"), index=False)
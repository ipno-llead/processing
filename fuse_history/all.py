import pandas as pd
import deba


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

    event_df = event_df[event_df["uid"].isin(per_df["uid"])]
    uof_df = uof_df[uof_df["uid"].isin(per_df["uid"])]
    sas_df = sas_df[sas_df["uid"].isin(per_df["uid"])]
    appeals_df = appeals_df[appeals_df["uid"].isin(per_df["uid"])]
    allegation_df = allegation_df[allegation_df["uid"].isin(per_df["uid"])]
    brady_df = brady_df[brady_df["uid"].isin(per_df["uid"])]
    post = post[post["uid"].isin(per_df["uid"])]
    
    uof_df.to_csv(deba.data("fuse_history/use_of_force.csv"), index=False)
    sas_df.to_csv(deba.data("fuse_history/stop_and_search.csv"), index=False)
    appeals_df.to_csv(deba.data("fuse_history/appeals.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse_history/allegation.csv"), index=False)
    brady_df.to_csv(deba.data("fuse_history/brady.csv"), index=False)
    post.to_csv(deba.data("fuse_history/post_officer_history.csv"), index=False)
    citizens.to_csv(deba.data("fuse_history/citizens.csv"), index=False)
    agencies.to_csv(deba.data("fuse_history/agency_reference_list.csv"), index=False)
    documents.to_csv(deba.data("fuse_history/documents.csv"), index=False)
    event_df.to_csv(deba.data("fuse_history/event.csv"), index=False)
    per_df.to_csv(deba.data("fuse_history/personnel.csv"), index=False)

import pandas as pd
import deba


if __name__ == "__main__":
    per_df = pd.read_csv(deba.data("match_history/personnel.csv"))
    allegation_df = pd.read_csv(deba.data("fuse_agency/allegation.csv"))
    uof_df = pd.read_csv(deba.data("fuse_agency/use_of_force.csv"))
    event_df = pd.read_csv(deba.data("match_history/event.csv"))
    sas_df = pd.read_csv(deba.data("fuse_agency/stop_and_search.csv"))
    appeals_df = pd.read_csv(deba.data("fuse_agency/appeals.csv"))
    citizens = pd.read_csv(deba.data("fuse_agency/citizens.csv"))
    agencies = pd.read_csv(deba.data("fuse_agency/agency_reference_list.csv"))
    documents = pd.read_csv(deba.data("clean/documents.csv"))
    post_df = pd.read_csv(deba.data("match_history/post_officer_history.csv"))

    allegation_df = allegation_df[allegation_df["uid"].isin(per_df["uid"])]
    uof_df = uof_df[uof_df["uid"].isin(per_df["uid"])]
    event_df = event_df[event_df["uid"].isin(per_df["uid"])]
    sas_df = sas_df[sas_df["uid"].isin(per_df["uid"])]
    appeals_df = appeals_df[appeals_df["uid"].isin(per_df["uid"])]
    post = post_df[post_df["uid"].isin(per_df["uid"])]

    event_df = event_df[~((event_df.agency.fillna("") == ""))]

    per_df.to_csv(deba.data("fuse/personnel.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event.csv"), index=False)
    uof_df.to_csv(deba.data("fuse/use_of_force.csv"), index=False)
    event_df.to_csv(deba.data("fuse/event.csv"), index=False)
    sas_df.to_csv(deba.data("fuse/stop_and_search.csv"), index=False)
    appeals_df.to_csv(deba.data("fuse/appeals.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse/allegation.csv"), index=False)
    citizens.to_csv(deba.data("fuse/citizens.csv"), index=False)
    agencies.to_csv(deba.data("fuse/agency_reference_list.csv"), index=False)
    documents.to_csv(deba.data("fuse/documents.csv"), index=False)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
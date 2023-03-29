import pandas as pd
import deba


if __name__ == "__main__":
    uof_df = pd.read_csv(deba.data("fuse_history/use_of_force.csv"))
    sas_df = pd.read_csv(deba.data("fuse_history/stop_and_search.csv"))
    appeals_df = pd.read_csv(deba.data("fuse_history/appeals.csv"))
    allegation_df = pd.read_csv(deba.data("fuse_history/allegation.csv"))
    brady_df = pd.read_csv(deba.data("fuse_history/brady.csv"))
    post = pd.read_csv(deba.data("fuse_history/post_officer_history.csv"))
    citizens = pd.read_csv(deba.data("fuse_history/citizens.csv"))
    agencies = pd.read_csv(deba.data("fuse_history/agency_reference_list.csv"))
    documents = pd.read_csv(deba.data("clean/documents.csv"))
    
    uof_df.to_csv(deba.data("fuse/use_of_force.csv"), index=False)
    sas_df.to_csv(deba.data("fuse/stop_and_search.csv"), index=False)
    appeals_df.to_csv(deba.data("fuse/appeals.csv"), index=False)
    allegation_df.to_csv(deba.data("fuse/allegation.csv"), index=False)
    brady_df.to_csv(deba.data("fuse/brady.csv"), index=False)
    post.to_csv(deba.data("fuse/post_officer_history.csv"), index=False)
    citizens.to_csv(deba.data("fuse/citizens.csv"), index=False)
    agencies.to_csv(deba.data("fuse/agency_reference_list.csv"), index=False)
    documents.to_csv(deba.data("fuse/documents.csv"), index=False)

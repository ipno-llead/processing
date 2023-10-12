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

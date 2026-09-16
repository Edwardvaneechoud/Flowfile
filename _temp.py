import flowfile as ff
from flowfile import col, when, lit

CSV_PATH = "answers.csv"
DS_VOTERS = ["alex", "auke", "volkan"]

def load_votes(path=CSV_PATH):
    votes = ff.read_csv(path, separator="|")
    votes = votes.rename({"#": "sticky_id",
                          "Sticky (as written on the board)": "sticky",
                          "Vote": "vote"})
    votes = votes.with_columns(                       # -> polars_code (Ticket 3)
        when(col("name").is_in(DS_VOTERS)).then(lit("DS")).otherwise(lit("DE")).alias("team"))
    votes = votes.with_columns(                       # -> polars_code (Ticket 2)
        col("vote").mean().over("name").alias("voter_mean"))
    votes = votes.with_columns(                       # -> formula (native ✓)
        (col("vote") / col("voter_mean")).alias("vote_corr"))
    return votes

def team_average(votes, value):
    per_team = votes.group_by(["sticky_id", "sticky", "team"]).agg(  # native ✓
        col(value).mean().alias("avg"))
    wide = per_team.pivot(on="team", index=["sticky_id", "sticky"], values="avg")  # native ✓
    wide = wide.rename({"DS": "ds_avg", "DE": "de_avg"})            # native ✓
    wide = wide.with_columns((col("ds_avg") - col("de_avg")).alias("gap"))         # formula ✓
    wide = wide.with_columns(                          # -> polars_code (Ticket 3)
        when(col("gap") <= -1.5).then(lit("DE much higher"))
        .when(col("gap") <= -0.67).then(lit("DE higher"))
        .when(col("gap") >= 1.5).then(lit("DS much higher"))
        .when(col("gap") >= 0.67).then(lit("DS higher"))
        .otherwise(lit("agree")).alias("reads_as"))
    wide = wide.with_columns([col("ds_avg").round(2), col("de_avg").round(2),
                              col("gap").round(2)])    # formula ✓
    wide = wide.with_columns(col("gap").abs().alias("gap_abs"))     # formula ✓
    wide = wide.sort(by="gap_abs", descending=True).drop("gap_abs") # sort ✓ ; drop -> code (Ticket 1)
    return wide

votes = load_votes()


from flowfile import open_graph_in_editor

open_graph_in_editor(votes.flow_graph)

raw = team_average(votes, "vote")
corrected = team_average(votes, "vote_corr")
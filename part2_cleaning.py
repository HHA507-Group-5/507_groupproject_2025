import os
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

# -----------------------------
# Environment & DB connection
# -----------------------------

load_dotenv()

sql_username = os.getenv('db_username')
sql_password = os.getenv('db_password')
sql_host = os.getenv('db_hostname')
sql_database = os.getenv('db_database')

if not all([sql_username, sql_password, sql_host, sql_database]):
    raise ValueError("Missing one or more database environment variables.")

url_string = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:3306/{sql_database}"
TABLE = "research_experiment_refactor_test"

engine = create_engine(url_string)

# -----------------------------
# Project metrics (from Part 1)
# -----------------------------

SELECTED_METRICS = [
    "Jump Height(m)",           # Hawkins
    "Peak Propulsive Power(W)", # Hawkins
    "distance_total",           # Kinexon
    "accel_load_accum",         # Kinexon
    "leftMaxForce",             # Vald
    "rightMaxForce"             # Vald
]

# -----------------------------
# Part 2.1 – Missing data, coverage, recency
# -----------------------------

def run_part2_1():
    metrics_sql = "(" + ",".join([f"'{m}'" for m in SELECTED_METRICS]) + ")"

    query = f"""
        SELECT playername, team, metric, value, timestamp
        FROM {TABLE}
        WHERE metric IN {metrics_sql}
    """
    df = pd.read_sql(query, engine)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    print("Rows loaded for selected metrics:", len(df))

    metric_stats = (
        df.groupby("metric")["value"]
        .agg(
            total_rows="size",
            null_count=lambda x: x.isna().sum(),
            zero_count=lambda x: (x == 0).sum()
        )
        .reset_index()
    )
    metric_stats["null_pct"] = metric_stats["null_count"] / metric_stats["total_rows"] * 100
    metric_stats["zero_pct"] = metric_stats["zero_count"] / metric_stats["total_rows"] * 100

    print("\nMissing / zero values by metric:")
    print(metric_stats)

    df_valid = df[
        df["value"].notna()
        & df["playername"].notna()
        & (df["playername"].str.strip() != "")
    ]

    counts = (
        df_valid
        .groupby(["team", "metric", "playername"])
        .size()
        .reset_index(name="measurement_count")
    )

    percentage_rows = []

    for (team, metric), group in counts.groupby(["team", "metric"]):
        total_players = group["playername"].nunique()
        players_5plus = group[group["measurement_count"] >= 5]["playername"].nunique()
        pct = (players_5plus / total_players * 100) if total_players > 0 else 0

        percentage_rows.append({
            "Team": team,
            "Metric": metric,
            "Total Players": total_players,
            "Players >=5": players_5plus,
            "Percent": pct
        })

    coverage_df = pd.DataFrame(percentage_rows)

    print("\nTeam x metric coverage (>= 5 measurements):")
    print(coverage_df.head(30))

    if not df_valid.empty:
        last_tests = (
            df_valid.groupby("playername")["timestamp"]
            .max()
            .reset_index(name="last_test_date")
        )

        latest_date = last_tests["last_test_date"].max()
        cutoff = latest_date - timedelta(days=180)

        not_recent = last_tests[last_tests["last_test_date"] < cutoff]

        print("\nAthletes not tested in last 6 months:", len(not_recent))
        print(not_recent.head(20))
    else:
        print("\nNo valid data for recent test check.")

# -----------------------------
# Part 2.2 – Long to wide transform
# -----------------------------

def transform_player_to_wide(playername, metrics=None):
    """
    Takes a player name and list of metrics.
    Returns a wide DataFrame: one row per timestamp, one column per metric.
    """
    if metrics is None:
        metrics = SELECTED_METRICS

    metrics_sql = "(" + ",".join([f"'{m}'" for m in metrics]) + ")"

    query = f"""
        SELECT playername, team, metric, value, timestamp
        FROM {TABLE}
        WHERE playername = '{playername}'
          AND metric IN {metrics_sql}
    """

    df_player = pd.read_sql(query, engine)

    if df_player.empty:
        print(f"No data found for {playername}")
        return df_player

    df_player["timestamp"] = pd.to_datetime(df_player["timestamp"])

    player_team = df_player["team"].iloc[0]
    player_name = df_player["playername"].iloc[0]

    wide = (
        df_player
        .pivot_table(
            index="timestamp",
            columns="metric",
            values="value",
            aggfunc="mean"
        )
        .reset_index()
        .sort_values("timestamp")
    )

    wide["team"] = player_team
    wide["playername"] = player_name

    return wide

# -----------------------------
# Part 2.3 – Derived metric vs team mean
# -----------------------------

def run_part2_3():
    metrics_sql = "(" + ",".join([f"'{m}'" for m in SELECTED_METRICS]) + ")"

    query = f"""
        SELECT playername, team, metric, value
        FROM {TABLE}
        WHERE metric IN {metrics_sql}
          AND team IS NOT NULL
          AND TRIM(team) <> ''
    """
    df = pd.read_sql(query, engine)

    df_valid = df[
        df["value"].notna()
        & df["playername"].notna()
        & (df["playername"].str.strip() != "")
    ]

    if df_valid.empty:
        print("\nNo valid data for Part 2.3.")
        return

    team_means = (
        df_valid
        .groupby(["team", "metric"])["value"]
        .mean()
        .reset_index(name="team_mean")
    )

    merged = df_valid.merge(team_means, on=["team", "metric"], how="left")

    merged = merged[merged["team_mean"] != 0]

    merged["percent_diff"] = (merged["value"] - merged["team_mean"]) / merged["team_mean"] * 100

    player_metric_avg = (
        merged
        .groupby(["playername", "team", "metric"])["percent_diff"]
        .mean()
        .reset_index(name="mean_percent_diff")
    )

    ranked = player_metric_avg.sort_values("mean_percent_diff", ascending=False)

    top5 = ranked.head(5)
    bottom5 = ranked.tail(5)

    print("\nTop 5 performers vs team mean (percent difference):")
    print(top5)

    print("\nBottom 5 performers vs team mean (percent difference):")
    print(bottom5)

# -----------------------------
# Main – Part 2.1, 2.2, 2.3
# -----------------------------

if __name__ == "__main__":
    try:
        run_part2_1()

        players_to_check = ["PLAYER_366", "PLAYER_143", "PLAYER_454"]

        for pid in players_to_check:
            print(f"\nWide-format data for {pid}:")
            wide_df = transform_player_to_wide(pid)
            print(wide_df.head())

        run_part2_3()

    finally:
        engine.dispose()
        print("\nDone with Part 2.1, 2.2, and 2.3.")

import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Loading environment variables from .env file
load_dotenv()

sql_username = os.getenv('db_username')
sql_password = os.getenv('db_password')
sql_host = os.getenv('db_hostname')
sql_database = os.getenv('db_database')

if not all([sql_username, sql_password, sql_host, sql_database]):
    raise ValueError("Missing database environment variables!")

# Creating the database connection string
url_string = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:3306/{sql_database}"

TABLE = "research_experiment_refactor_test"

# Creating engine
engine = create_engine(url_string)

# Selected metrics
SELECTED_METRICS = [
    "Jump Height(m)",
    "Peak Propulsive Power(W)",
    "distance_total",
    "accel_load_accum",
    "leftMaxForce",
    "rightMaxForce"
]

# Flagging thresholds
DECLINE_THRESHOLD = 0.10  # 10% drop
INACTIVITY_DAYS = 30 # inactive if not tested in 30+ days
TEAM_NORM_SD = 2 # outside 2 standard deviations from team mean
ASYMMETRY_THRESHOLD = 0.10  # 10% left/right difference

# Load data for selected metrics (only relevant metrics and non-null values)
metrics_sql = "(" + ", ".join(f"'{m}'" for m in SELECTED_METRICS) + ")"
query = f"""
    SELECT playername, team, metric, value, timestamp
    FROM {TABLE}
    WHERE metric IN {metrics_sql}
      AND playername IS NOT NULL
      AND team IS NOT NULL
      AND value IS NOT NULL
"""
df = pd.read_sql(query, engine)

# Makes sure timestamp column is in datetime format
df["timestamp"] = pd.to_datetime(df["timestamp"])

# Flagging functions:

# Identifys athletes who have not been tested in more than cutoff_days
def flag_inactivity(player_df, cutoff_days=INACTIVITY_DAYS):
    last_test = player_df.groupby("playername")["timestamp"].max().reset_index()
    cutoff = pd.Timestamp.today() - pd.Timedelta(days = cutoff_days)
    # Flag players with last test before the cutoff date
    inactive = last_test[last_test["timestamp"] < cutoff]
    inactive["flag_reason"] = f"Inactive > {cutoff_days} days"
    # Add team info from original dataset
    inactive = inactive.merge(df[["playername", "team"]].drop_duplicates(), on="playername", how="left")
    # Metrics and values are NA for inactivity flags
    inactive["metric"] = None
    inactive["metric_value"] = None
    inactive["last_test_date"] = inactive["timestamp"]
    return inactive[["playername","team","metric","flag_reason","metric_value","last_test_date"]]

# Flag athletes who drops more than a threshold percentage of 10% from their rolling mean of the last 3 tests
def flag_performance_decline(player_df, decline_threshold=DECLINE_THRESHOLD):
    rows = []
    for player in player_df["playername"].unique():
        sub = player_df[player_df["playername"] == player].copy()
        for metric in sub["metric"].unique():
            metric_sub = sub[sub["metric"] == metric].sort_values("timestamp")
            # Require at least 3 prior tests to calculate rolling mean
            if len(metric_sub) < 3:
                continue
            # Rolling mean of last 3 tests
            rolling_mean = metric_sub["value"].rolling(3).mean()
            # Flag if latest value drops below threshold from rolling mean
            latest_val = metric_sub["value"].iloc[-1]
            # Avoid division by zero
            if rolling_mean.iloc[-1] == 0:
                continue
            drop_pct = (rolling_mean.iloc[-1] - latest_val) / rolling_mean.iloc[-1]
            # Flag if decline exceeds threshold
            if drop_pct > decline_threshold:
                rows.append({
                    "playername": player,
                    "team": metric_sub["team"].iloc[-1],
                    "metric": metric,
                    "flag_reason": f"Declined > {decline_threshold*100:.0f}%",
                    "metric_value": latest_val,
                    "last_test_date": metric_sub["timestamp"].iloc[-1]
                })
    return pd.DataFrame(rows)

# Identify athletes with metric values that are 2 SD outside the team mean
def flag_team_norm(df, n_sd=TEAM_NORM_SD):
    rows = []
    for metric in df["metric"].unique():
        metric_sub = df[df["metric"] == metric]
        team_mean = metric_sub.groupby("team")["value"].transform("mean")
        team_std = metric_sub.groupby("team")["value"].transform("std").replace(0, np.nan)
        outliers = ((metric_sub["value"] - team_mean).abs() > n_sd * team_std)
        flagged = metric_sub[outliers]
        for _, row in flagged.iterrows():
            rows.append({
                "playername": row["playername"],
                "team": row["team"],
                "metric": row["metric"],
                "flag_reason": f"Outside team norm ±{n_sd} SD",
                "metric_value": row["value"],
                "last_test_date": row["timestamp"]
            })
    return pd.DataFrame(rows)

# Identify athletes with left/right asymmetry exceeding threshold for bilateral metrics (leftMaxForce vs rightMaxForce)
def flag_asymmetry(df, threshold=ASYMMETRY_THRESHOLD):
    rows = []
    bilateral_pairs = [("leftMaxForce", "rightMaxForce")]
    for left, right in bilateral_pairs:
        sub = df[df["metric"].isin([left, right])]
        for player in sub["playername"].unique():
            player_sub = sub[sub["playername"] == player]
            left_val = player_sub[player_sub["metric"] == left]["value"].mean()
            right_val = player_sub[player_sub["metric"] == right]["value"].mean()
            # Skip if either value is missing
            if pd.isna(left_val) or pd.isna(right_val):
                continue
            diff = abs(left_val - right_val) / max(left_val, right_val)
            if diff > threshold:
                rows.append({
                    "playername": player,
                    "team": player_sub["team"].iloc[0],
                    "metric": f"{left}/{right}",
                    "flag_reason": f"Asymmetry > {threshold*100:.0f}%",
                    "metric_value": f"{left_val:.2f}/{right_val:.2f}",
                    "last_test_date": player_sub["timestamp"].max()
                })
    return pd.DataFrame(rows)

# Apply flagging functions to data
df_flags = pd.concat([
    flag_inactivity(df),
    flag_performance_decline(df),
    flag_team_norm(df, n_sd=TEAM_NORM_SD),
    flag_asymmetry(df)
], ignore_index=True)

# Export results to CSV
df_flags.to_csv("part4_flagged_athletes.csv", index=False)

# Close engine
engine.dispose()
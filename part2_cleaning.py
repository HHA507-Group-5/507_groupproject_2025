# ================================================================
# PART 2 – DATA CLEANING & TRANSFORMATION
# ================================================================

import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ------------------------------------------------
# LOAD ENVIRONMENT VARIABLES + CONNECT TO DATABASE
# ------------------------------------------------

load_dotenv(".env")   # Make 100% sure .env is loaded

sql_username = os.getenv("db_username")
sql_password = os.getenv("db_password")
sql_host     = os.getenv("db_hostname")
sql_database = os.getenv("db_database")

if not all([sql_username, sql_password, sql_host, sql_database]):
	raise ValueError("Missing one or more database environment variables!")

# Create DB URL
url_string = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:3306/{sql_database}"

# Table name
TABLE = "research_experiment_refactor_test"

# Connect
engine = create_engine(url_string)

print(f"\nConnected successfully to database: {sql_database}")
print(f"Using table: {TABLE}")

# ------------------------------------------------
# YOUR SELECTED METRICS
# ------------------------------------------------

VALUE_COL = "value"  # numeric column

SELECTED_METRICS = [
	"Jump Height (m)",           # Hawkins
	"Peak Propulsive Power (W)", # Hawkins
	"distance_total",            # Kinexon
	"accel_load_accum",          # Kinexon
	"MaxForce_left",             # Vald
	"MaxForce_right",            # Vald
]

# ------------------------------------------------
# HELPER: Build SQL-safe
# ------------------------------------------------

def _build_metrics_in_clause(metrics):
	escaped = [m.replace("'", "''").replace("%", "%%") for m in metrics]
	return "(" + ",".join(f"'{m}'" for m in escaped) + ")"

# ------------------------------------------------
# 2.1 – MISSING DATA ANALYSIS
# ------------------------------------------------

def missing_data_analysis(selected_metrics):
	print("\n===== PART 2.1 — Missing Data Analysis =====")

	metrics_sql = _build_metrics_in_clause(selected_metrics)

	# 1. Missing + zero counts
	q_missing = f"""
		SELECT
			metric,
			SUM(CASE WHEN {VALUE_COL} IS NULL THEN 1 ELSE 0 END) AS null_count,
			SUM(CASE WHEN {VALUE_COL} = 0 THEN 1 ELSE 0 END) AS zero_count,
			COUNT(*) AS total_rows
		FROM {TABLE}
		WHERE metric IN {metrics_sql}
		GROUP BY metric
		ORDER BY (null_count + zero_count) DESC;
	"""
	missing_df = pd.read_sql(q_missing, engine)
	print("\nNULL + ZERO counts by metric:")
	print(missing_df)

	# 2. % of athletes per team with >= 5 measurements
	q_team = f"""
		SELECT playername, team, metric, timestamp, {VALUE_COL} AS value
		FROM {TABLE}
		WHERE metric IN {metrics_sql}
		  AND playername IS NOT NULL AND TRIM(playername) <> ''
		  AND team IS NOT NULL AND TRIM(team) <> '';
	"""
	df = pd.read_sql(q_team, engine)

	if df.empty:
		print("\nNo rows found for these metrics!")
		return

	counts = (
		df.groupby(["team", "playername"])
		  .size()
		  .reset_index(name="n_measurements")
	)
	counts["has_5_plus"] = counts["n_measurements"] >= 5

	team_summary = counts.groupby("team").agg(
		total_athletes=("playername", "nunique"),
		athletes_ge5=("has_5_plus", "sum")
	)
	team_summary["pct_with_5_plus"] = (
		team_summary["athletes_ge5"] / team_summary["total_athletes"] * 100
	)

	print("\n% of athletes with ≥ 5 measurements per team:")
	print(team_summary)

	# 3. Not tested in last 6 months
	df["timestamp"] = pd.to_datetime(df["timestamp"])
	last_test = df.groupby("playername")["timestamp"].max().reset_index()
	cutoff = pd.Timestamp.today() - pd.DateOffset(months=6)

	inactive = last_test[last_test["timestamp"] < cutoff]

	# add team
	latest_team = (
		df.sort_values("timestamp")
		  .drop_duplicates("playername", keep="last")[["playername", "team"]]
	)
	inactive = inactive.merge(latest_team, on="playername", how="left")

	print(f"\nAthletes not tested since {cutoff.date()}:")
	print(inactive)

	# 4. Data sufficiency
	print("\nData sufficiency:")
	print(f"Total rows for selected metrics: {len(df)}")
	print(f"Total athletes: {df['playername'].nunique()}")
	print(f"Median rows per athlete: {df.groupby('playername').size().median():.2f}")

# ------------------------------------------------
# 2.2 — LONG → WIDE TRANSFORMATION
# ------------------------------------------------

def long_to_wide_for_player(player_name, selected_metrics):
	metrics_sql = _build_metrics_in_clause(selected_metrics)

	q = f"""
		SELECT timestamp, metric, {VALUE_COL} AS value
		FROM {TABLE}
		WHERE playername = :p
		  AND metric IN {metrics_sql}
		ORDER BY timestamp;
	"""
	df = pd.read_sql(text(q), engine, params={"p": player_name})

	if df.empty:
		print(f"\nNo data for player {player_name}.")
		return pd.DataFrame(columns=["timestamp"] + list(selected_metrics))

	df["timestamp"] = pd.to_datetime(df["timestamp"])

	wide = (
		df.pivot_table(
			index="timestamp",
			columns="metric",
			values="value",
			aggfunc="mean"
		)
		.reset_index()
		.sort_values("timestamp")
	)

	# ensure all chosen metrics appear
	for m in selected_metrics:
		if m not in wide.columns:
			wide[m] = pd.NA

	return wide[["timestamp"] + list(selected_metrics)]

def test_long_to_wide_on_three_players(selected_metrics):
	print("\n===== PART 2.2 — Long → Wide Transformation =====")

	metrics_sql = _build_metrics_in_clause(selected_metrics)

	q_players = f"""
		SELECT DISTINCT playername, team
		FROM {TABLE}
		WHERE metric IN {metrics_sql}
		  AND playername IS NOT NULL AND TRIM(playername) <> ''
		  AND team IS NOT NULL AND TRIM(team) <> ''
		LIMIT 200;
	"""
	players = pd.read_sql(q_players, engine)

	if players.empty:
		print("No players found for these metrics.")
		return

	sample = players.drop_duplicates("team").head(3)

	for _, row in sample.iterrows():
		p = row["playername"]
		t = row["team"]
		wide = long_to_wide_for_player(p, selected_metrics)
		print(f"\nPlayer: {p} | Team: {t}")
		print(wide.head())

# ------------------------------------------------
# 2.3 — DERIVED METRICS (TEAM COMPARISON)
# ------------------------------------------------

def derived_metric_analysis(selected_metrics):
	print("\n===== PART 2.3 — Derived Team Metrics =====")

	metrics_sql = _build_metrics_in_clause(selected_metrics)

	q = f"""
		SELECT playername, team, metric, timestamp, {VALUE_COL} AS value
		FROM {TABLE}
		WHERE metric IN {metrics_sql}
		  AND playername IS NOT NULL AND TRIM(playername) <> ''
		  AND team IS NOT NULL AND TRIM(team) <> '';
	"""
	df = pd.read_sql(q, engine)

	if df.empty:
		print("No rows found.")
		return

	df["timestamp"] = pd.to_datetime(df["timestamp"])
	df = df.dropna(subset=["value"])

	# team averages
	df["team_metric_mean"] = df.groupby(["team", "metric"])["value"].transform("mean")

	# % difference above/below team mean
	df["pct_diff_team_mean"] = (
		(df["value"] - df["team_metric_mean"]) / df["team_metric_mean"] * 100
	)

	athlete_summary = (
		df.groupby(["team", "playername", "metric"])["pct_diff_team_mean"]
		  .mean()
		  .reset_index(name="avg_pct_diff")
	)

	top5 = athlete_summary.sort_values("avg_pct_diff", ascending=False).head(5)
	bottom5 = athlete_summary.sort_values("avg_pct_diff", ascending=True).head(5)

	print("\nTop 5 performers (% ABOVE team mean):")
	print(top5)

	print("\nBottom 5 performers (% BELOW team mean):")
	print(bottom5)

	# optional: z-scores
	df["z_score"] = df.groupby(["team", "metric"])["value"].transform(
		lambda x: (x - x.mean()) / (x.std(ddof=0) or 1)
	)

	print("\nExample z-scores (first 10 rows):")
	print(df[["playername", "team", "metric", "value", "z_score"]].head(10))

# ------------------------------------------------
# Wrapper to run all of Part 2
# ------------------------------------------------

def run_part2(selected_metrics):
	missing_data_analysis(selected_metrics)
	test_long_to_wide_on_three_players(selected_metrics)
	derived_metric_analysis(selected_metrics)

if __name__ == "__main__":
    run_part2(SELECTED_METRICS)
    engine.dispose()

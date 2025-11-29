import pandas as pd
import numpy as np
from scipy.stats import zscore
 
# ---------------------------------------
# Selected Metrics (with system sources)
# ---------------------------------------
# Hawkins
JUMP_HEIGHT = "Jump Height (m)"
PEAK_PROP_POWER = "Peak Propulsive Power (W)"
 
# Kinexon
TOTAL_DISTANCE = "Total Distance (distance_total)"
ACCEL_LOAD = "Accumulated Acceleration Load (accel_load_accum)"
 
# Vald Strength Testing
MAX_FORCE_LR = "Max Force (MaxForce; left/right)"
 
SELECTED_METRICS = [
    JUMP_HEIGHT,         # Hawkins
    PEAK_PROP_POWER,     # Hawkins
    TOTAL_DISTANCE,      # Kinexon
    ACCEL_LOAD,          # Kinexon
    MAX_FORCE_LR         # Vald
]
 
# =============================================================================
# 2.1 Missing Data Analysis (Group)
# =============================================================================
 
def missing_data_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each selected metric:
      - total rows
      - rows with NULL or zero values
      - percent missing/zero
    """
    summary = {}
    df_sel = df[df["metric"].isin(SELECTED_METRICS)].copy()
 
    for metric in SELECTED_METRICS:
        metric_df = df_sel[df_sel["metric"] == metric]
        total = len(metric_df)
        missing = metric_df[metric_df["value"].isna() | (metric_df["value"] == 0)]
        summary[metric] = {
            "total_rows": total,
            "missing_or_zero": len(missing),
            "percent_missing": round(len(missing) / total * 100, 2)
            if total > 0 else np.nan,
        }
 
    return pd.DataFrame(summary).T.sort_values("percent_missing", ascending=False)
 
 
def athletes_with_min_measurements_by_sport(df: pd.DataFrame, min_tests: int = 5) -> pd.DataFrame:
    """
    For each sport, calculate what % of athletes have at least `min_tests`
    measurements across the selected metrics.
    """
    df_sel = df[df["metric"].isin(SELECTED_METRICS)].copy()
 
    counts = (
        df_sel
        .groupby(["sport", "athlete"])["timestamp"]
        .count()
        .reset_index(name="num_measurements")
    )
 
    counts["has_min"] = counts["num_measurements"] >= min_tests
 
    result = (
        counts.groupby("sport")["has_min"]
        .mean()
        .reset_index(name="percent_with_min")
    )
    result["percent_with_min"] *= 100
 
    return result
 
 
def athletes_with_min_measurements_by_sport_team(df: pd.DataFrame, min_tests: int = 5) -> pd.DataFrame:
    """
    For each sport/team, calculate what % of athletes have at least `min_tests`
    measurements across the selected metrics.
    """
    df_sel = df[df["metric"].isin(SELECTED_METRICS)].copy()
 
    counts = (
        df_sel
        .groupby(["sport", "team", "athlete"])["timestamp"]
        .count()
        .reset_index(name="num_measurements")
    )
 
    counts["has_min"] = counts["num_measurements"] >= min_tests
 
    result = (
        counts.groupby(["sport", "team"])["has_min"]
        .mean()
        .reset_index(name="percent_with_min")
    )
    result["percent_with_min"] *= 100
 
    return result
 
 
def athletes_missing_6_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify athletes who have NOT been tested in the last 6 months
    for the selected metrics.
    """
    df_sel = df[df["metric"].isin(SELECTED_METRICS)].copy()
    df_sel["timestamp"] = pd.to_datetime(df_sel["timestamp"])
 
    # Last test per athlete (for selected metrics only)
    latest = df_sel.groupby("athlete")["timestamp"].max().reset_index()
 
    global_max = df_sel["timestamp"].max()
    cutoff = global_max - pd.Timedelta(days=180)
 
    latest["missing_6_months"] = latest["timestamp"] < cutoff
 
    return latest[latest["missing_6_months"]]
 
 
def check_sufficiency(df: pd.DataFrame, min_tests_per_metric: int = 5):
    """
    Simple heuristic for 'do we have enough data to answer the question?':
      - For each athlete & each selected metric, count tests.
      - An athlete is 'sufficient' if they have at least `min_tests_per_metric`
        measurements for EVERY selected metric.
    Returns:
      per_athlete_sufficient: Series[bool] indexed by athlete
      summary_stats: Series with simple summary %s
    """
    df_sel = df[df["metric"].isin(SELECTED_METRICS)].copy()
 
    counts = (
        df_sel
        .groupby(["athlete", "metric"])["timestamp"]
        .count()
        .unstack("metric", fill_value=0)
    )
 
    # Booleans per athlete (True = enough data for all metrics)
    per_athlete_sufficient = (counts >= min_tests_per_metric).all(axis=1)
 
    summary_stats = pd.Series({
        "num_athletes_total": int(counts.shape[0]),
        "num_athletes_sufficient": int(per_athlete_sufficient.sum()),
        "percent_athletes_sufficient": round(per_athlete_sufficient.mean() * 100, 2)
            if counts.shape[0] > 0 else np.nan
    })
 
    return per_athlete_sufficient, summary_stats
 
# =============================================================================
# 2.2 Data Transformation Challenge (long -> wide)
# =============================================================================
 
def make_wide(df: pd.DataFrame, athlete_name: str, metrics=SELECTED_METRICS) -> pd.DataFrame:
    """
    Transform long -> wide for a single athlete:
      - Input: df, athlete name, list of metrics
      - Output: DataFrame with columns:
          timestamp, [metrics...]
        one row per test session
      - Properly handles missing values: missing metric values will appear as NaN.
    """
    tmp = df[
        (df["athlete"] == athlete_name)
        & (df["metric"].isin(metrics))
    ].copy()
 
    tmp["timestamp"] = pd.to_datetime(tmp["timestamp"])
 
    wide = (
        tmp.pivot_table(
            index="timestamp",
            columns="metric",
            values="value",
            aggfunc="mean"       # if multiple rows per timestamp/metric, average them
        )
        .reset_index()
        .sort_values("timestamp")
    )
 
    # Ensure all selected metrics appear as columns, even if athlete never had that metric
    for m in metrics:
        if m not in wide.columns:
            wide[m] = np.nan
 
    # Reorder columns: timestamp first, then metrics
    wide = wide[["timestamp"] + list(metrics)]
 
    return wide
 
# =============================================================================
# 2.3 Derived Metric: Percent Difference vs Team Mean
# =============================================================================
 
def add_team_mean_and_diff(df: pd.DataFrame) -> pd.DataFrame:
    """
    For selected metrics:
      - Calculate team mean per metric
      - For each row, compute percent difference from team mean
        (value - team_mean) / team_mean * 100
    """
    df_sel = df[df["metric"].isin(SELECTED_METRICS)].copy()
 
    team_means = (
        df_sel
        .groupby(["team", "metric"])["value"]
        .mean()
        .reset_index(name="team_mean")
    )
 
    merged = df_sel.merge(team_means, on=["team", "metric"], how="left")
 
    merged["percent_diff"] = (
        (merged["value"] - merged["team_mean"]) / merged["team_mean"]
    ) * 100
 
    return merged
 
 
def top_bottom_performers(df_with_diff: pd.DataFrame, top_n: int = 5):
    """
    Given a DataFrame with 'percent_diff' (from add_team_mean_and_diff),
    identify top and bottom performers:
      - Aggregated by athlete (mean percent_diff across selected metrics)
      - Returns: (top_n, bottom_n) DataFrames
    """
    df_clean = df_with_diff[df_with_diff["metric"].isin(SELECTED_METRICS)].copy()
 
    ranking = (
        df_clean
        .groupby("athlete")["percent_diff"]
        .mean()
        .reset_index()
        .sort_values("percent_diff", ascending=False)
    )
 
    topN = ranking.head(top_n)
    bottomN = ranking.tail(top_n)
 
    return topN, bottomN
 
 
def add_z_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional: add z-scores by metric for selected metrics.
    """
    df_filtered = df[df["metric"].isin(SELECTED_METRICS)].copy()
    df_filtered["z_score"] = (
        df_filtered
        .groupby("metric")["value"]
        .transform(zscore)
    )
    return df_filtered
 
# =============================================================================
# Example MAIN EXECUTION (for demo/testing)
# =============================================================================
 
if __name__ == "__main__":
    # Load your raw data
    # df must contain at least: athlete, metric, value, timestamp, team, sport
    df = pd.read_csv("raw_data.csv")
 
    # 2.1 Missing Data Analysis ----------------------------------------------
    print("\n=== 2.1 Missing Data Analysis ===")
 
    print("\n--- Missing Data Summary (sorted by % missing) ---")
    mds = missing_data_summary(df)
    print(mds)
 
    print("\n--- % Athletes with ≥5 Measurements by Sport ---")
    sport_pct = athletes_with_min_measurements_by_sport(df, min_tests=5)
    print(sport_pct)
 
    print("\n--- % Athletes with ≥5 Measurements by Sport & Team ---")
    sport_team_pct = athletes_with_min_measurements_by_sport_team(df, min_tests=5)
    print(sport_team_pct)
 
    print("\n--- Athletes Missing ≥6 Months (Selected Metrics) ---")
    miss6 = athletes_missing_6_months(df)
    print(miss6)
 
    print("\n--- Data Sufficiency Check (per athlete + summary) ---")
    per_athlete_sufficient, suff_summary = check_sufficiency(df, min_tests_per_metric=5)
    print("Per-athlete sufficiency (True = enough data for all selected metrics):")
    print(per_athlete_sufficient.head())
    print("\nSufficiency summary:")
    print(suff_summary)
 
    # 2.2 Data Transformation (long -> wide) --------------------------------
    print("\n=== 2.2 Long -> Wide Transformation ===")
 
    # Example: test on at least 3 players
    athletes_to_test = ["PLAYER_001", "PLAYER_047", "PLAYER_139"]
 
    for a in athletes_to_test:
        print(f"\n--- Wide Format for {a} ---")
        wide_a = make_wide(df, a, metrics=SELECTED_METRICS)
        print(wide_a.head())
 
    # 2.3 Derived Metric vs Team Mean ---------------------------------------
    print("\n=== 2.3 Derived Metrics vs Team Averages ===")
 
    df_with_diff = add_team_mean_and_diff(df)
    top5, bottom5 = top_bottom_performers(df_with_diff, top_n=5)
 
    print("\n--- Top 5 Performers (avg % diff vs team mean) ---")
    print(top5)
 
    print("\n--- Bottom 5 Performers (avg % diff vs team mean) ---")
    print(bottom5)
 
    # Optional z-scores example
    print("\n--- Sample with Z-Scores (optional) ---")
    df_z = add_z_scores(df)
    print(df_z.head())
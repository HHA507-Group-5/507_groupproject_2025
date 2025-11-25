import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Loading environment variables from .env file
load_dotenv()

sql_username = os.getenv('db_username')
sql_password = os.getenv('db_password')
sql_host = os.getenv('db_hostname')
sql_database = os.getenv('db_database')

if not all([sql_username, sql_password, sql_host, sql_database]):
    raise ValueError("Missing one or more database environment variables.")

# Creating the database connection string
url_string = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:3306/{sql_database}"

TABLE = "research_experiment_refactor_test"

# Creating engine
engine = create_engine(url_string)

def preview_table():
    print("\n=== Testing Connection: Showing First 50 Rows ===")
    query = "SELECT * FROM research_experiment_refactor_test LIMIT 50"
    df = pd.read_sql(query, engine)
    print(df.head())
    return df

def data_quality_assessment():
    print("\n=== DATA QUALITY ASSESSMENT (Part 1.2) ===")

    # 1. Unique athletes
    q1 = f"SELECT COUNT(DISTINCT playername) AS unique_players FROM {TABLE}"
    print("Unique athletes:", pd.read_sql(q1, engine).iloc[0, 0])

    # 2. Unique teams
    q2 = f"""
        SELECT COUNT(DISTINCT team) AS unique_teams
        FROM {TABLE}
        WHERE team IS NOT NULL AND team <> ''
    """
    print("Unique teams:", pd.read_sql(q2, engine).iloc[0, 0])

    # 3. Date range
    q3 = f"SELECT MIN(timestamp) AS earliest, MAX(timestamp) AS latest FROM {TABLE}"
    print("\nDate range:")
    print(pd.read_sql(q3, engine))

    # 4. Data source record counts
    q4 = f"""
        SELECT data_source, COUNT(*) AS count
        FROM {TABLE}
        GROUP BY data_source
        ORDER BY count DESC
    """
    print("\nRecord count by data source:")
    print(pd.read_sql(q4, engine))

    # 5. Invalid names
    q5 = f"""
        SELECT playername, COUNT(*) AS row_count
        FROM {TABLE}
        WHERE playername IS NULL
           OR TRIM(playername) = ''
           OR UPPER(playername) = 'UNKNOWN'
        GROUP BY playername
    """
    invalid = pd.read_sql(q5, engine)
    print("\nInvalid names:")
    if invalid.empty:
        print("None")
    else:
        print(invalid)

    # 6. Players with data from multiple sources
    q6 = f"""
        SELECT playername,
               COUNT(DISTINCT data_source) AS source_count
        FROM {TABLE}
        GROUP BY playername
        HAVING source_count >= 2
        ORDER BY source_count DESC
    """
    multi = pd.read_sql(q6, engine)
    print("\nAthletes with ≥2 data sources:", len(multi))
    print(multi.head(10))

def metric_discovery():
    print("\n=== METRIC DISCOVERY (Part 1.3) ===")

    def top_metrics(source):
        q = f"""
            SELECT metric, COUNT(*) AS count
            FROM {TABLE}
            WHERE data_source = '{source}'
            GROUP BY metric
            ORDER BY count DESC
            LIMIT 10
        """
        df = pd.read_sql(q, engine)
        print(f"\nTop 10 metrics for {source}:")
        print(df)
        return df

    hawkins_top = top_metrics("hawkins")   
    kinexon_top = top_metrics("kinexon")
    vald_top    = top_metrics("vald")

    # Total unique metrics across all sources
    q_unique = f"""
        SELECT COUNT(DISTINCT metric) AS unique_metrics
        FROM {TABLE}
    """
    unique_df = pd.read_sql(q_unique, engine)
    print("\nTotal unique metrics across all sources:")
    print(unique_df)

# For each data source, show date range + record count for TOP metrics
    def metric_summary_for_source(source, top_df):
        if top_df.empty:
            return None

        # Build a safe IN (...) list with proper quoting and escaping
        metrics = top_df["metric"].tolist()

        # Escape single quotes and % for each metric name
        escaped_metrics = [
            m.replace("'", "''").replace("%", "%%") for m in metrics
        ]

        # Build: ('metric1','metric2',...)
        metrics_sql = "(" + ",".join(f"'{m}'" for m in escaped_metrics) + ")"

        q = f"""
            SELECT
                metric,
                COUNT(*) AS record_count,
                MIN(timestamp) AS earliest_date,
                MAX(timestamp) AS latest_date
            FROM {TABLE}
            WHERE data_source = '{source}'
              AND metric IN {metrics_sql}
            GROUP BY metric
            ORDER BY record_count DESC;
        """

        df = pd.read_sql(q, engine)
        print(f"\nDate Range + Record Count for Top Metrics ({source}):")
        print(df)
        return df

    hawkins_summary = metric_summary_for_source("hawkins", hawkins_top)
    kinexon_summary = metric_summary_for_source("kinexon", kinexon_top)
    vald_summary    = metric_summary_for_source("vald", vald_top)

if __name__ == "__main__":
    preview_table()
    data_quality_assessment()
    metric_discovery()

    engine.dispose()
    print("\nDone.")

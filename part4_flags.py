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
    raise ValueError("Missing one or more database environment variables.")

# Creating the database connection string 
url_string = f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:3306/{sql_database}"
TABLE = "research_experiment_refactor_test"

# Creating engine
engine = create_engine(url_string)


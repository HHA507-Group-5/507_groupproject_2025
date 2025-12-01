# HHA 507 Health Informatics Group Project: Athletics Performance Analytics

## Team members:
* Angel Huang
* Anita Liu
* Ashley McGowan
* Aarav Desai

**Presenation:** 


## Overview
This project analyzes collegiate athletics database containing performance metrics, collected from multiple tracking systems (Hawkins force plates, Kinexon GPS/accelerometry, and Vald strength testing). The goal is to extract insights about athlete performance, monitor trends over time, and develop a flagging system to identify athletes needing attention.

The project uses Python and SQL for data extraction, cleaning, transformation, exploratory analysis, visualization, and performance monitoring.

## Database Overview

The athletics database contains performance data from multiple tracking systems integrated into a single unified table.

### Main Table:
**`research_experiment_refactor_test`** - Single unified table containing all performance metrics from three data sources:
- Hawkins (force plates)
- Kinexon (GPS/accelerometry)
- Vald (strength testing)

### Table Schema:

| Column | Type | Description |
|--------|------|-------------|
| **id** | BIGINT | Unique record identifier (auto-increment) |
| **playername** | VARCHAR(255) | Anonymized player identifier (e.g., PLAYER_001, PLAYER_002) |
| **timestamp** | DATETIME | Date and time of the measurement/session |
| **device** | VARCHAR(50) | Specific device/equipment used for measurement |
| **metric** | VARCHAR(255) | Name of the performance metric being measured |
| **value** | DECIMAL(20,6) | Numeric value of the metric |
| **team** | VARCHAR(255) | Sport/team affiliation (e.g., Football, Soccer, Basketball) |
| **session_type** | VARCHAR(255) | Type of session (e.g., Practice, Game, Training) - only relevant for Kinexon |
| **session_description** | TEXT | Detailed description of the session |
| **function_description** | VARCHAR(255) | Movement or exercise description |
| **data_source** | VARCHAR(50) | Original data source (Hawkins, Kinexon, or Vald) |
| **created_at** | TIMESTAMP | Record creation timestamp |

## File Structure:
```
507_groupproject_2025/
├── README.md (with group member names, roles, and contributions)
├── references.md (full bibliography in APA or similar format)
├── .env.example (template for database credentials - DO NOT include actual credentials)
├── .gitignore (exclude .env, data files, etc.)
├── part1_exploration.py
├── part1_summary.pdf
├── part1_literature_review.pdf (NEW - your metric selection and lit review)
├── part2_cleaning.py
├── part3_viz_individual.ipynb
├── part3_viz_comparison.ipynb
├── part4_flags.py
├── part4_flagged_athletes.csv
├── part4_flag_justification.pdf (NEW - explain your thresholds)
├── part4_research_synthesis.pdf (NEW - replaces sport_analysis.pdf)
└── final_presentation.pdf
```

## Setup instructions:
1. Clone the repository using 
```bash
git clone https://github.com/<your-username>/507_groupproject_2025.git
cd 507_groupproject_2025

```
2. Install Python dependencies
```bash
pip install pandas sqlalchemy pymysql matplotlib seaborn numpy scipy python-dotenv
```
### Required Python Libraries:
```python
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
import pymysql
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from scipy import stats
```

## Database connection instructions:
1. Make a `.venv` and `.env`
2. Copy `.env.example` to .env
3. Fill in your database credentials:
```bash
DB_HOST=your_database_host
DB_USER=your_username
DB_PASSWORD=your_password
DB_NAME=database_name
DB_TABLE=research_experiment_refactor_test
```
4. Create a new .py file and follow the template below
### Database Connection Template:
```bash
from sqlalchemy import create_engine
import pandas as pd

# Connection string (credentials will be provided)
engine = create_engine(
    "mysql+pymysql://username:password@host:port/database_name"
)

# Example query
query = "SELECT * FROM research_experiment_refactor_test LIMIT 10"
df = pd.read_sql(query, engine)

# Close connection when done
engine.dispose()
```
5. Run the code in terminal by typing
```bash
python <name_of_file>.py
```
### Team Database Connection Screenshots:
Angel
![screenshot](screenshots/angelconnection.png) 

Anita
![screenshot](screenshots/anitaconnection.png) 

Ashley
![screenshot](screenshots/ashleyconnection.png) 

Aarav
![screenshot](screenshots/aaravconnection.png) 

## How to Run Scripts
`part1_exploration.py`
1. Open `part1_exploration.py`
2. Run python part1_exploration.py in the terminal
3. Results:
    - Connects to the database and previews the first 50 rows
    - Performs data quality assessment (unique athletes/teams, date range, invalid names, multiple sources)
    - List top 10 metrics for each data source
    - Show date ranges and record counts for top 10 metrics

`part2_cleaning.py`
1. Open `part2_cleaning.py`
2. Run python part2_cleaning.py in the terminal
3. Results should show cleaned dataset that
    - Filters data for selected metrics
    - Analyzes missing values, duplicates, and bad timestamps for selected metrics
    - Transforms long format data to wide format

`part3_viz_comparison.ipynb`
1. Open `part3_viz_individual.ipynb`
2. Run all cells to:
    - Load two selected athletes
    - Create line plots of their metrics over the last 6–12 months
    - Identify their best and worst performance dates
    - Calculate simple trend lines to see improvement or decline

`part3_viz_individual.ipynb`
1. Open `part3_viz_comparison.ipynb`
  - Run all cells to:
    - Create box plots comparing the selected metric between two teams
    - Run a t-test (or similar) to check for statistical differences
    - Plot testing frequency over time by team

- In the same `part3_viz_comparison.ipynb`, run the dashboard section (Part 3.3 cells).
  - This will:
    - Summarize total tests per month
    - Show a stacked bar chart by data source
    - Highlight gaps or unusual patterns in data collection

`part4_flags.py`
1. Open `part4_flags.py`
2. Run python part4_flags.py in the terminal
3. Results:
    - Applys flags for: 
        - inactivity (>30 days)
        - performance decline (>10%)
        - outside team norms (2 SD)
        - asymmetry (>10%)
    - Exports flagged athletes to `part4_flagged_athletes.csv`





 

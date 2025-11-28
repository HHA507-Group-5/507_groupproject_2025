# HHA 507 Health Informatics Group Project: Athletics Performance Analytics

## Team members:
* Angel Huang
* Anita Liu
* Ashley McGowan
* Aarav Desai

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
`part1_exploration.py`\
Connects to the database, performs data quality assessment and metric discovery
1. 
2. 
3. 

`part2_cleaning.py`\
Cleans data, handles missing values, transforms long to wide format, creates derived metrics
1. 
2. 
3. 

`part3_viz_comparison.ipynb`\
Compares teams and metrics, performs statistical analysis
1. 
2. 
3. 

`part3_viz_individual.ipynb`\
Plots individual athlete trends over time
1. 
2. 
3. 

`part4_flags.py`\
Implements performance monitoring flag system, outputs flagged athletes
1. 
2. 
3. 





 

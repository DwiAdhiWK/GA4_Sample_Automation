# GA4 Automation Pipeline
Simulates a scheduled ETL automation using GA4 sample e-commerce data (January 2021).

## Tech Stack

- **Language**: Python
- **Core Libraries**: pandas, schedule, gspread, Kaggle API (`kaggle`)
- **Destination**: Google Sheets (used as data source for visualization)

## Data Source
- Kaggle dataset: [GA4 Obfuscated Sample E-commerce (Jan 2021)](https://www.kaggle.com/datasets/pdaasha/ga4-obfuscated-sample-ecommerce-jan2021)

## Features

- Complete ETL pipeline for GA4-style session and event data
- Scheduled automation that runs the full ETL process periodically

## How the Automation Works

The entire workflow is wrapped in a single main function called `script()` that is intended to be scheduled (e.g. via `schedule` library, cron, or cloud scheduler).

It consists of three clearly separated stages:

```text
download_data() → process_data() → upload_to_sheets()
```

1. download_data()

Authenticates and downloads the GA4 sample dataset via the Kaggle API
Saves the raw files into a local folder: kaggle_dataset/

2. process_data()

Transformation phase — split into two logical steps:
Converts raw event-level data into a clean event-based DataFrame
Aggregates events into a session-based DataFrame (most useful format for reporting and visualization)

Handles cleaning, type conversion, and meaningful aggregations

3. upload_to_sheets()

Loads the final cleaned session DataFrame into a Google Sheet
The sheet serves as a live data source for dashboards (e.g. in Looker Studio)

## Insight

Analysis based on 42 sessions from the GA4 sample e-commerce data (January 2021):

- **Traffic Sources**  
  Google / organic generated the highest volume with **15 sessions** (36% of total). Of these, **3 were engaged sessions**, resulting in a strong **20.0% engagement rate** — the best-performing channel in the dataset.

- **Device Breakdown**  
  - Desktop: **28 sessions** (67%)  
  - Mobile: **14 sessions** (33%)  
  → Desktop users significantly outnumbered mobile users in this e-commerce sample, suggesting a preference for personal computers over handheld devices.  
  → However, both device categories showed identical **14.3% engagement rates**, indicating no meaningful difference in session quality.

- **Geographic Insights**  
  Only two countries appear in the data:  
  - United States: **28 sessions** (high volume) but a low **7.1% engagement rate**  
  - Qatar: **14 sessions** (lower volume) but a surprisingly strong **28.6% engagement rate**  
  → This highlights potential regional differences in user behavior or content relevance, with Qatar users showing much higher engagement despite fewer overall visits.

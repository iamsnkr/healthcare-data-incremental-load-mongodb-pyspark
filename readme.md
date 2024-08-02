# Healthcare Data Analysis with Spark

This Spark application processes healthcare data stored in CSV format on HDFS and pushes the transformed data to Manogo DB for further analysis and applications.

## Features

- **Data Validation and Cleaning:**
  - Ensures all mandatory columns (`patient_id`, `age`, `diagnosis_code`, etc.) are present and in correct formats.
  - Handles missing or inconsistent data appropriately.

- **Transformations:**
  - **Disease Gender Ratio:**
    - Calculates gender distribution for each disease to identify gender prevalence.
  - **Top Diseases:**
    - Identifies the top 3 most common diseases based on the dataset.
  - **Age Category Analysis:**
    - Categorizes patients into age groups (`30-40`, `41-50`, etc.) and analyzes disease prevalence within each group.
  - **Flag for Senior Patients:**
    - Flags patients aged 60 years and older for special healthcare attention.
  - **Disease Trend Over Week:**
    - Analyzes the number of cases diagnosed for each disease across different days of the week to identify trends.

## Data Management

- **Data Storage:**
  - Transformed data from each Spark job is pushed to Manogo DB for centralized storage and further analysis.


## Project Structure

```
├───data
│   ├───processed
│   └───raw
│           health_data_20230801.csv
│           health_data_20230802.csv
│           health_data_20230803.csv
│           health_data_20230804.csv
│           health_data_20230805.csv
│           mock-data-generator.py
│
└───src
        config.properties
        healthcare-data-analysis.py
        __init__.py

```


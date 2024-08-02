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
  - **Console Output**

```bash
org.mongodb.spark#mongo-spark-connector_2.12 added as a dependency
Setting default log level to "WARN".

Displaying first 5 rows of the raw data DataFrame:
+----------+---+------+--------------+---------------------+--------------+
|patient_id|age|gender|diagnosis_code|diagnosis_description|diagnosis_date|
+----------+---+------+--------------+---------------------+--------------+
|        P1| 41|     M|          D123|             Diabetes|    2023-08-01|
|        P2| 42|     F|          C345|               Cancer|    2023-08-01|
|        P3| 56|     F|          H234|  High Blood Pressure|    2023-08-01|
|        P4| 54|     M|          C345|               Cancer|    2023-08-01|
|        P5| 35|     M|          C345|               Cancer|    2023-08-01|
+----------+---+------+--------------+---------------------+--------------+
only showing top 5 rows

Data saved successfully to MongoDB collection 'healthcare_db.healthcare_raw_data'.

Mandatory columns verification is completed

Mandatory columns casted successfully to respective datatypes

Duplicate records verification is completed
{'patient_id': 0, 'age': 0, 'gender': 0, 'diagnosis_code': 0, 'diagnosis_description': 0, 'diagnosis_date': 0}

Handling null and missing values process is completed
cleaning_report : {'missing_columns': [], 'duplicate_records': 0, 'columns_with_null_count': {'patient_id': 0, 'age': 0, 'gender': 0, 'diagnosis_code': 0, 'diagnosis_description': 0, 'diagnosis_date': 0}}

Cleaning_report data saved successfully to MongoDB collection 'healthcare_db.cleaning_report'.

Gender_ratio data saved successfully to MongoDB collection 'healthcare_db.disease_gender_ratio'.

Common_diseases data saved successfully to MongoDB collection 'healthcare_db.most_common_diseases'.

Age_category data saved successfully to MongoDB collection 'healthcare_db.age_category'.

Senior_citizen_flag data saved successfully to MongoDB collection 'healthcare_db.senior_citizen_flag'.

Disease_trend_over_the_week data saved successfully to MongoDB collection 'healthcare_db.disease_trend_over_the_week'.
```

## Data Management

- **Data Storage:**
  - Transformed data from each Spark job is pushed to Manogo DB for centralized storage and further analysis.
  - ![alt text](/data/processed/mongoDB_data.png)


## Project Structure

```
├───data
│   ├───processed
│   │       mongoDB_data.png
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


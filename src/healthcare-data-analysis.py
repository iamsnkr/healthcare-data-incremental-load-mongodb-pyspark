import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import configparser as cf

config = cf.ConfigParser()
config.read("config.properties", 'utf_8')


# get the required properties
def get_date_filepath():
    data = [config['DATE_FILEPATH']['file_path'],
            config['DATE_FILEPATH']['file_date']]
    return f"{data[0]}health_data_{data[1]}.csv"


# Creating the spark instance
spark_session = SparkSession.builder \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .master('local').appName('HealthCareDataAnalysis').getOrCreate()

# database and collection details
database_name = "healthcare_db"
collection_name = "healthcare_raw_data"


def read_and_save_raw_data(spark: SparkSession) -> DataFrame:
    # Read the data from the file
    df = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load(get_date_filepath())

    print("Displaying first 5 rows of the raw data DataFrame:")
    df.show(5)

    # Writing Raw data to MongoDB
    df.write.option('database', database_name) \
        .option('collection', collection_name) \
        .format('mongo').mode('append').save()

    print(f"Data saved successfully to MongoDB collection '{database_name}.{collection_name}'.")

    return df


def data_quality_check(df: DataFrame) -> Tuple[DataFrame, Dict]:
    # performing data validation and data cleaning steps
    mandatory_fields = ['patient_id', 'age', 'gender', 'diagnosis_code', 'diagnosis_description', 'diagnosis_date']
    missing_col = []
    # Check all the mandatory columns are there or not
    for mandatory_col in mandatory_fields:
        if mandatory_col not in df.columns:
            missing_col.append(mandatory_col)
            raise ValueError(f"Mandatory column {mandatory_col} is missing from the DataFrame")

    print("Mandatory columns verification is completed")

    # Cast all columns to required data formats
    df = df.withColumn('patient_id', col('patient_id').cast('string')) \
        .selectExpr('patient_id',
                    'cast(age as int) age',
                    'cast(gender as string) gender',
                    'cast(diagnosis_code as string) diagnosis_code',
                    'cast(diagnosis_description as string) diagnosis_description',
                    'diagnosis_date')

    print("Mandatory columns casted successfully to respective datatypes")
    # Check the duplicate records in the data frame
    duplicate_records = df.groupBy(mandatory_fields).count().filter(col('count') > 2).count()
    if duplicate_records > 1:
        print(f"{duplicate_records} duplicate records in the DataFrame")

    print("Duplicate records verification is completed")
    # Let's check the null values count
    null_count = json.loads(df.select([sum(col(c).isNull().cast('int')).alias(c) for c in df.columns]).toJSON().first())
    print(null_count)
    # Handle missing values
    # For Patient ID dropping the null values when patientId is missing
    df = df.filter(col('patient_id').isNotNull())
    # For age filling null values with median
    age_median = df.filter(col('age').isNotNull()).agg(median('age').alias('median')).first()[0]
    df = df.fillna({'age': age_median})
    print(f"age_median : {age_median}")
    # For diagnosis_date filling null values with latest date
    latest_date = df.filter(col('diagnosis_date').isNotNull()).sort(desc('diagnosis_date')).first()[0]
    df = df.fillna({'diagnosis_date': latest_date})
    print(f"latest_date : {latest_date}")

    print("Handling null and missing values process is completed")
    cleaning_report = {
        'missing_columns': missing_col,
        'duplicate_records': duplicate_records,
        'columns_with_null_count': null_count
    }
    print(f"cleaning_report : {cleaning_report}")

    return df, cleaning_report


"""
Transformation: 1
Disease Gender Ratio: Calculate the gender ratio for each disease. 
This will help in identifying if a particular disease is more prevalent in a particular gender. 
For example, for each diagnosis_code, you could calculate the ratio of male to female patients.

Output: 
+--------------+-----+
|diagnosis_code|ratio|
+--------------+-----+
|          C345| 1.17|
|          D123| 0.67|
|          H234| 1.44|
+--------------+-----+
"""


def disease_gender_ratio(df: DataFrame):
    disease_gender_ratio_df = df.groupBy('diagnosis_code').pivot('gender').count() \
        .select('diagnosis_code', round((col('M') / col('F')), 2).alias("ratio"))

    # Writing Raw data to MongoDB
    disease_gender_ratio_df.write.option('database', database_name) \
        .option('collection', 'disease_gender_ratio_data') \
        .format('mongo').mode('append').save()
    print(f"Gender_ratio data saved successfully to MongoDB collection '{database_name}.disease_gender_ratio'.")


"""
Transformation: 2
Most Common Diseases: Find the top 3 most common
diseases in the dataset. This will help in identifying the most
prevalent diseases.

Output:
+-------------------+-----+
|            disease|count|
+-------------------+-----+
|High Blood Pressure|   39|
|           Diabetes|   35|
|             Cancer|   26|
+-------------------+-----+
"""


def most_common_diseases(df: DataFrame):
    most_common_diseases_df = df.groupBy(col('diagnosis_description').alias('disease')) \
        .agg(count('*').alias('count')).sort(desc('count'))
    # Writing Raw data to MongoDB
    most_common_diseases_df.write.option('database', database_name) \
        .option('collection', 'most_common_diseases_data') \
        .format('mongo').mode('append').save()
    print(f"Common_diseases data saved successfully to MongoDB collection '{database_name}.most_common_diseases'.")


"""
Transformation: 3
Age Category: 
Create age categories. 
For example, you can divide age into groups like '30-40', '41-50', '51-60', '61-70' and so forth. 
Then, calculate the number of patients in each age category for each disease. 
This can help understand the age distribution of different diseases

Output:
+---------+---------------------+--------------+-----+
|age_group|diagnosis_description|diagnosis_code|count|
+---------+---------------------+--------------+-----+
|    30-40|  High Blood Pressure|          H234|   11|
|    30-40|             Diabetes|          D123|    9|
|    30-40|               Cancer|          C345|    8|
|    41-50|  High Blood Pressure|          H234|   13|
|    41-50|             Diabetes|          D123|   11|
+---------+---------------------+--------------+-----+
"""


def age_category(df: DataFrame):
    age_category_df = df.withColumn("age_group",
                                    when((col('age') >= 30) & (col('age') <= 40), '30-40') \
                                    .when((col('age') >= 41) & (col('age') <= 50), '41-50') \
                                    .when((col('age') >= 51) & (col('age') <= 60), '51-60') \
                                    .when((col('age') >= 61) & (col('age') <= 70), '61-70') \
                                    .when((col('age') >= 71), '70+') \
                                    .otherwise('minors')) \
        .groupBy('age_group', 'diagnosis_description', 'diagnosis_code') \
        .count().alias('count').orderBy('age_group', desc('count'))
    # Writing Raw data to MongoDB
    age_category_df.write.option('database', database_name) \
        .option('collection', 'age_category_data') \
        .format('mongo').mode('append').save()
    print(f"Age_category data saved successfully to MongoDB collection '{database_name}.age_category'.")


"""
Transformation: 4
Flag for senior patients: 
Flag patients who are senior citizens (typically, age >= 60 years). 
This might be useful information since senior citizens might require 
special healthcare attention or follow-up.

Output:
+----------+---+------+---------------------+-------------------+
|patient_id|age|gender|diagnosis_description|senior_citizen_flag|
+----------+---+------+---------------------+-------------------+
|       P58| 70|     F|             Diabetes|                  Y|
|       P88| 69|     F|             Diabetes|                  Y|
|       P71| 67|     F|             Diabetes|                  Y|
|       P90| 67|     F|             Diabetes|                  Y|
|       P62| 66|     M|  High Blood Pressure|                  Y|
+----------+---+------+---------------------+-------------------+
"""


def senior_citizen_flag(df: DataFrame):
    senior_citizen_flag_df = df.withColumn("senior_citizen_flag",
                                           when(col('age') >= 60, 'Y') \
                                           .otherwise('N')) \
        .select('patient_id', 'age', 'gender', 'diagnosis_description', 'senior_citizen_flag') \
        .sort(desc('age'))
    # Writing Raw data to MongoDB
    senior_citizen_flag_df.write.option('database', database_name) \
        .option('collection', 'senior_citizen_flag_data') \
        .format('mongo').mode('append').save()
    print(f"Senior_citizen_flag data saved successfully to MongoDB collection '{database_name}.senior_citizen_flag'.")


"""
Transformation: 5
Disease trend over the week: 
If you have more than a week's data, you can calculate the number of cases of each
disease for each day of the week to understand if there's a
trend (more cases diagnosed on particular days).

Output:
+---------------------+---------+-----+
|diagnosis_description|dayofweek|count|
+---------------------+---------+-----+
|               Cancer|        6|   46|
|             Diabetes|        7|   42|
|  High Blood Pressure|        3|   39|
|             Diabetes|        5|   37|
|  High Blood Pressure|        4|   36|
+---------------------+---------+-----+
"""


def disease_trend_over_the_week(spark: SparkSession):
    path = config['DATE_FILEPATH']['file_path'] + "*csv"
    # Read all files data at a time
    df = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load(path)

    disease_trend_over_the_week_df = df.withColumn("dayofweek", dayofweek('diagnosis_date')) \
        .groupBy('diagnosis_description', 'dayofweek') \
        .agg(count('*').alias('count')) \
        .orderBy(desc('count'), 'dayofweek')

    # Writing Raw data to MongoDB
    disease_trend_over_the_week_df.write.option('database', database_name) \
        .option('collection', 'disease_trend_over_the_week_data') \
        .format('mongo').mode('append').save()
    print(f"""Disease_trend_over_the_week data saved successfully to MongoDB collection 
    '{database_name}.disease_trend_over_the_week'.""")


def save_cleaning_report(spark: SparkSession, cleaning_report: dict):
    schema = StructType([
        StructField("missing_columns", ArrayType(StringType()), True),
        StructField("duplicate_records", IntegerType(), True),
        StructField("columns_with_null_count", StructType([
            StructField("patient_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", IntegerType(), True),
            StructField("diagnosis_code", IntegerType(), True),
            StructField("diagnosis_description", IntegerType(), True),
            StructField("diagnosis_date", IntegerType(), True)
        ]), True)
    ])
    spark.createDataFrame([cleaning_report], schema=schema).show(5)
    spark.createDataFrame([cleaning_report], schema=schema) \
        .write.option('database', database_name) \
        .option('collection', 'cleaning_report') \
        .format('mongo').mode('append').save()
    print(f"Cleaning_report data saved successfully to MongoDB collection '{database_name}.cleaning_report'.")


def save_processing_date_info():
    processing_date = config['DATE_FILEPATH']['file_date']
    processing_date = config['DATE_FILEPATH']['file_date']
    date_obj = datetime.strptime(processing_date, '%Y%m%d') + timedelta(days=1)
    next_processing_date = datetime.strftime(date_obj, '%Y%m%d')
    config.set('DATE_FILEPATH', 'file_date', next_processing_date)
    with open("config.properties", "w") as f:
        config.write(f)


try:
    raw_data_df = read_and_save_raw_data(spark_session)
    list_info = data_quality_check(raw_data_df)
    quality_data_df = list_info[0]
    print(type(list_info[1]))
    save_cleaning_report(spark_session, list_info[1])
    disease_gender_ratio(quality_data_df)
    most_common_diseases(quality_data_df)
    age_category(quality_data_df)
    senior_citizen_flag(quality_data_df)
    disease_trend_over_the_week(spark_session)
    save_processing_date_info()

except Exception as e:
    # Print detailed exception information
    print(f"An error occurred: {str(e)}")
    print(f"Exception type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
finally:
    spark_session.stop()

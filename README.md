# AWS Glue & PySpark ETL Pipeline

This project demonstrates a serverless ETL pipeline using AWS Glue and PySpark to process phishing email data.
The transformed data is stored in Amazon S3 in Parquet format, partitioned by label, cataloged with AWS Glue Data Catalog, and queried through Amazon Athena.


# Project Objective

To build a real-world ETL pipeline that processes a phishing email dataset using AWS managed services, applying data cleaning, transformation, and partitioning best practices for efficient SQL analytics.


# Tech Stack

• AWS Glue (Job + Crawler) – ETL orchestration and schema discovery

• PySpark – Data cleaning and transformation

• Amazon S3 – Data lake storage (raw & curated zones)

• AWS Glue Data Catalog – Metadata management

• Amazon Athena – SQL-based querying over S3 data

• AWS IAM – Access and permissions management


# Dataset

• Source: Kaggle – Email Phishing Dataset

• Format: CSV

• S3 Object: s3://data-engineer-projects-jota/email_phishing_data.csv

• Volume: ~520,000 records

• Columns:

num_words

num_links

num_stopwords

num_spelling_errors

label (0 = not phishing, 1 = phishing)


# ETL Pipeline Overview

### 1 - Data Ingestion

• The raw dataset (email_phishing_data.csv) was uploaded to the following S3 path: s3://data-engineer-projects-jota/email_phishing_data.csv

### 2 - Data Transformation with AWS Glue (PySpark)

df = spark.read.option("header", True).csv("s3://data-engineer-projects-jota/email_phishing_data.csv")

df_clean = df.dropna()

from pyspark.sql.functions import col
df_clean = df_clean.withColumnRenamed("label", "phishing_label")
df_clean = df_clean.withColumn("phishing_label", col("phishing_label").cast("int"))

df_clean.write.mode("overwrite").partitionBy("phishing_label").parquet("s3://data-engineer-projects-jota/projeto2/curated/")

### 3 - Glue Crawler Configuration

• Data Source: s3://data-engineer-projects-jota/projeto2/curated/

• Database: projeto3_jota

• Generated Table: projeto2

### 4 - Athena Queries

SELECT * FROM projeto3_jota.projeto2 LIMIT 10;

SELECT phishing_label, COUNT(*) AS total
FROM projeto3_jota.projeto2
GROUP BY phishing_label;


# Key Learnings

• Building a serverless ETL pipeline using AWS Glue and PySpark

• Converting CSV files into partitioned Parquet datasets in S3

• Automating schema creation with AWS Glue Crawler

• Performing optimized SQL queries via Amazon Athena

• Managing permissions through IAM roles and policies

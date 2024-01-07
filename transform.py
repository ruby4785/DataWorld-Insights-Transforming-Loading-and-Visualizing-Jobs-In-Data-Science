import sys
from pyspark.sql import SparkSession
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window
from pyspark.sql.functions import col, percent_rank, when
from pyspark.sql.functions import udf
from pyspark.sql.types import *

spark = SparkSession.builder.appName("airflow_with_emr").getOrCreate()

def main():
    s3_location = "s3://data-jobs/input_folder/"
    
    # Correcting the column name and removing extra colon
    data_jobs = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3_location).toDF(
        'work_year', 'job_title', 'job_category', 'salary_currency', 'salary', 'salary_in_usd', 
        'employee_residence', 'experience_level', 'employment_type', 'work_setting', 'company_location', 'company_size'
    )
    

   # Convert 'salary_in_usd' column to numeric
    data_jobs = data_jobs.withColumn('salary_in_usd', col('salary_in_usd').cast('double'))

    # Create 'experience_level_numeric' column
    data_jobs = data_jobs.withColumn('experience_level_numeric',
                                    when(col('experience_level') == 'Entry-level', 1)
                                    .when(col('experience_level') == 'Mid-level', 2)
                                    .when(col('experience_level') == 'Senior', 3)
                                    .when(col('experience_level') == 'Executive', 4)
                                    .otherwise(0)
                                    )

    # Create the 'salary_experience_ratio' column
    data_jobs = data_jobs.withColumn('salary_experience_ratio', col('salary_in_usd') / col('experience_level_numeric'))

    # Define bin edges and labels
    bin_edges = [0, 50000, 100000, 200000, 300000, 400000, 500000]
    bin_labels = ['0-50K', '50k-100k', '100k-200k', '200k-300k', '300k-400k', '400k-500k']

    # Create a Bucketizer instance
    bucketizer = Bucketizer(splits=bin_edges, inputCol="salary_in_usd", outputCol="salary_bin")


    # Transform the data using the bucketizer
    data_jobs = bucketizer.transform(data_jobs)

    # Create a new column 'salary_bin_label' based on 'salary_bin'
    salary_range = {0:"0-50K", 1:"50k-100k", 2:"100k-200k", 3: "200k-300k", 4:"300k-400k", 5:"400k-500k"}
    udf_salary = udf(lambda x: salary_range[x], StringType())

    data_jobs=data_jobs.withColumn("salary_bin_label", udf_salary("salary_bin"))
        
   
    # Write the DataFrame to Parquet
    data_jobs.coalesce(1).write.format("parquet").mode('overwrite').save("s3://data-jobs/output_folder/")

main()

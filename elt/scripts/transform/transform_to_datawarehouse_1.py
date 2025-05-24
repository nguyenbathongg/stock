import sys
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
import os

def process(parquet_file_path):
    # Create SparkSession with local file system configuration
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_companies)") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse") \
        .config("spark.hadoop.fs.defaultFS", "file:///") \
        .master("local[*]") \
        .getOrCreate()

    # Ensure we use local file system by adding file:// prefix
    if not parquet_file_path.startswith("file://"):
        parquet_file_path = "file://" + os.path.abspath(parquet_file_path)

    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(parquet_file_path)

    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()

    # Convert Spark DataFrame to Pandas DataFrame
    df_pandas = df_spark.toPandas()

    # Connect to DuckDB
    database_path = '/home/thong/WorkSpace/Project/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)

    # Register the Pandas DataFrame as a DuckDB table and insert data into dim_companies
    conn.register('df_pandas', df_pandas)
    conn.execute('''
        INSERT INTO dim_companies (
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        ) SELECT 
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        FROM df_pandas
    ''')
    print("Data inserted into dim_companies successfully!")

    # Close DuckDB connection
    conn.close()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    # Check if the Parquet file path is provided as a command-line argument
    if len(sys.argv) != 2:
        print("Usage: python transform_to_datawarehouse_1.py <parquet_file_path>")
        print("Example: python transform_to_datawarehouse_1.py /path/to/your/file.parquet")
        sys.exit(1)
    
    # Get the Parquet file path from command-line arguments
    parquet_file_path = sys.argv[1]
    
    # Check if the file exists
    if not os.path.exists(parquet_file_path):
        print(f"Error: File '{parquet_file_path}' does not exist.")
        sys.exit(1)
    
    # Process the Parquet file and insert data into DuckDB
    process(parquet_file_path)
import sys
import duckdb
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import os

def process(parquet_file_path):
    # Create SparkSession with local file system configuration
    spark = SparkSession.builder \
        .appName("Insert Parquet into DuckDB (dim_times, fact_candles)") \
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
    
    # Rename columns for clarity
    df_spark = df_spark.withColumnRenamed("T", "company_ticket") \
        .withColumnRenamed("v", "volume") \
        .withColumnRenamed("vw", "volume_weighted") \
        .withColumnRenamed("o", "open") \
        .withColumnRenamed("c", "close") \
        .withColumnRenamed("h", "high") \
        .withColumnRenamed("l", "low") \
        .withColumnRenamed("t", "time_stamp") \
        .withColumnRenamed("n", "num_of_trades") \
        .withColumnRenamed("otc", "is_otc")
    
    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()
    
    # Convert Spark DataFrame to Pandas DataFrame
    df_pandas = df_spark.toPandas()
    print(f"Loaded {len(df_pandas)} records from parquet file")
    print(df_pandas.head())

    # Check if we have any data to process
    if df_pandas.empty:
        print("Error: No data found in the parquet file. Exiting.")
        spark.stop()
        sys.exit(1)
    
    # Get yesterday's date
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")
    
    # Connect to DuckDB
    database_path = '/home/thong/WorkSpace/Project/datawarehouse.duckdb'
    conn = duckdb.connect(database=database_path)
    
    # Insert new time data into dim_time if it does not exist
    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        SELECT
            '{yesterday}',
            '{yesterday.strftime("%A")}',
            '{yesterday.strftime("%B")}',
            '{((yesterday.month - 1) // 3) + 1}',
            {yesterday.year}
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_time WHERE date = '{yesterday}'
        )
    ''')
    
    # Get corresponding time_id from dim_time
    id_time_df = conn.execute(f'''
        SELECT time_id FROM dim_time WHERE date = '{yesterday}'
    ''').fetchdf()
    
    # Check if time_id was found
    if id_time_df.empty:
        print(f"Error: No time_id found for date {yesterday} in dim_time table")
        print("Available dates in dim_time:")
        available_dates = conn.execute("SELECT date FROM dim_time ORDER BY date DESC LIMIT 5").fetchdf()
        print(available_dates)
        conn.close()
        spark.stop()
        sys.exit(1)
    
    time_id = id_time_df['time_id'].iloc[0]
    print(f"Found time_id: {time_id} for date: {yesterday}")
    
    # Get corresponding company_id from dim_companies
    id_company_df = conn.execute(f'''
        SELECT company_id, company_ticket FROM dim_companies
    ''').fetchdf()
    print(f"Found {len(id_company_df)} companies in dim_companies table")
    
    # Create a new DataFrame containing company_id and company_ticket from dim_companies
    companies_df = id_company_df.drop_duplicates(subset=['company_ticket'], keep='last')
    print(f"After removing duplicates: {len(companies_df)} unique companies")
    
    # Join companies_df to df_pandas to get corresponding company_id
    df_pandas = df_pandas.merge(companies_df, on='company_ticket', how='left')
    
    # Check for companies that couldn't be matched
    unmatched_companies = df_pandas[df_pandas['company_id'].isnull()]
    if not unmatched_companies.empty:
        print("Warning: Found records with unmatched company tickets:")
        print(unmatched_companies['company_ticket'].unique())
        print("These records will be skipped.")
    
    # Filter out records without valid company_id
    df_pandas = df_pandas[df_pandas['company_id'].notnull()]
    
    # Check if we have any data left to process
    if df_pandas.empty:
        print("Error: No valid records found after company matching. Exiting.")
        conn.close()
        spark.stop()
        sys.exit(1)
    
    print(f"Processing {len(df_pandas)} valid records.")
    
    # Add candles_time_id to DataFrame
    df_pandas['candles_time_id'] = time_id
    print(df_pandas.head())
    
    # Load DataFrame into fact_candles table
    conn.register('df_pandas', df_pandas)
    
    try:
        conn.execute('''
            INSERT INTO fact_candles (
                candle_volume,
                candle_volume_weighted,
                candle_open,
                candle_close,
                candle_high,
                candle_low,
                candle_time_stamp,
                candle_num_of_trades,
                candle_is_otc,
                candles_time_id,
                candle_company_id
            ) SELECT 
                volume,
                volume_weighted,
                open,
                close,
                high,
                low,
                time_stamp,
                num_of_trades,
                is_otc,
                candles_time_id,
                company_id
            FROM df_pandas
        ''')
        print(f"Data inserted into fact_candles successfully! {len(df_pandas)} records processed.")
    except Exception as e:
        print(f"Error inserting data into fact_candles: {e}")
        conn.close()
        spark.stop()
        sys.exit(1)
    
    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    # Check if the Parquet file path is provided as a command-line argument
    if len(sys.argv) != 2:
        print("Usage: python transform_to_datawarehouse_2.py <parquet_file_path>")
        print("Example: python transform_to_datawarehouse_2.py /path/to/your/file.parquet")
        sys.exit(1)
    
    # Get the Parquet file path from command-line arguments
    parquet_file_path = sys.argv[1]
    
    # Check if the file exists
    if not os.path.exists(parquet_file_path):
        print(f"Error: File '{parquet_file_path}' does not exist.")
        sys.exit(1)
    
    # Process the Parquet file and insert data into DuckDB
    process(parquet_file_path)
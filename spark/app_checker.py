from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, countDistinct
from datetime import datetime, timedelta
import argparse

def get_spark():
    """
    Initializes and returns a Spark session.
    Sets the log level to ERROR to reduce verbosity in the logs.
    """
    global spark
    builder = SparkSession.builder
    spark = builder.appName('time_series').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_parser():
    """
    Sets up and returns an argument parser for command-line arguments.
    Returns:
        argparse.ArgumentParser: Configured argument parser with required arguments.
    """
    parser = argparse.ArgumentParser(description='Process and filter app data over a date range.')
    
    parser.add_argument('--data_path', required=True, type=str, 
                        help='S3 path to daily aggregated data (e.g., s3://ada-prod-data/etl/data/brq/agg/agg_brq/daily/)')
    parser.add_argument('--app_list_file', required=True, type=str, 
                        help='S3 path to pre-segmented app data (e.g., s3://ada-business-insights/data/reference/ref_persona/personas_list.csv)')
    parser.add_argument('--start_date', required=True, type=str, help='Start date (format: YYYYMMDD)')
    parser.add_argument('--end_date', required=True, type=str, help='End date (format: YYYYMMDD)')
    parser.add_argument('--result_path', required=True, type=str, 
                        help='S3 path to save results (e.g., s3://ada-dev/app_category/ref/)')

    return parser

def read_compare(input_list, app_list_df):
    """
    Reads parquet data from the input list, processes it, and compares with the app list DataFrame.
    
    Args:
        input_list (list): List of file paths for the input data.
        app_list_df (DataFrame): Spark DataFrame containing app list data.
    
    Returns:
        DataFrame: Filtered DataFrame of apps that are in the input data but not in the app list.
    """
    # Read and select relevant columns from input parquet data
    df = spark.read.parquet(*input_list)
    df_app = df.select('ifa', F.explode('app').alias('asn'))
    df_app = df_app.select('ifa', 'asn.bundle')
    
    # Group by 'bundle' to get unique counts of IFAs per app
    df_app = df_app.groupBy("bundle").agg(countDistinct("ifa").alias("unique_ifa"))
    df_app = df_app.withColumnRenamed("bundle", "bundle_app")
    
    # Left join with app_list_df and filter apps not found in app_list_df
    result = df_app.join(app_list_df, df_app["bundle_app"] == app_list_df["bundle"], "left")
    filtered_df = result.filter(col("bundle").isNull())
    result_df = filtered_df.select("bundle_app", "unique_ifa")
    
    return result_df

if __name__ == '__main__':
    # Initialize Spark session and argument parser
    spark = get_spark()
    parser = get_parser()
    args = parser.parse_args()

    # Parse command-line arguments
    data_path = args.data_path
    app_list_file = args.app_list_file
    start_date = args.start_date
    end_date = args.end_date
    result_path = args.result_path

    # Generate list of dates within the specified range
    date_format = '%Y%m%d'
    start = datetime.strptime(start_date, date_format)
    end = datetime.strptime(end_date, date_format)
    date_list = []
    current_date = start
    while current_date <= end:
        date_list.append(current_date.strftime(date_format))
        current_date += timedelta(days=1)

    # List of countries to process (can be extended or modified)
    country_list = ['MY', 'TH', 'PH', 'ID']

    # Load and filter app list data
    app_list_df = spark.read.parquet(app_list_file)
    app_list_df = app_list_df.filter(
        (col("category").isNotNull()) & 
        (col("category") != 'Not Found') & 
        (col("category") != '')
    )

    # Process each country's data and accumulate results
    app_list_not = None
    for country in country_list:
        print(f"Loading data for country: {country}")
        input_list = [f"{data_path}{country}/{date}" for date in date_list]
        print(f"List of paths: {input_list}")

        df = read_compare(input_list, app_list_df)
        if app_list_not is None:
            app_list_not = df
        else:
            app_list_not = app_list_not.union(df)

    # Finalize and save results to specified path
    app_list_not = app_list_not.withColumnRenamed("bundle_app", "bundle")
    app_list_not.coalesce(1).write.csv(result_path, mode='overwrite', header=True)

    # Stop the Spark session
    spark.stop()

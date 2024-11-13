from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, when
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
    parser = argparse.ArgumentParser(description='Data processing for segment grouping and filtering.')
    
    parser.add_argument('--old_result', required=True, type=str, 
                        help='Path to old reference data (e.g., s3://ada-business-insights/data/reference/ref_persona/personas_list.csv)')
    parser.add_argument('--new_result', required=True, type=str, 
                        help='Path to new result data (e.g., s3://ada-dev/app_category/temp_result/)')

    return parser

if __name__ == '__main__':
    # Initialize Spark session and argument parser
    spark = get_spark()
    parser = get_parser()
    args = parser.parse_args()

    # Parse command-line arguments
    old_result = args.old_result
    new_result = args.new_result

    # Load the old data from parquet file, strip column names, and filter required columns
    df = spark.read.parquet(old_result)
    df = df.toDF(*[c.strip() for c in df.columns])
    df_ref = df.select('segment_grp_1', 'segment_index').dropDuplicates().filter(df.segment_index.isNotNull())

    # Load the new data from CSV file and drop 'segment_index' column
    df_new = spark.read.csv(new_result, header=True)
    df_new = df_new.drop('segment_index')

    # Merge with reference data (df_ref) on 'segment_grp_1' column
    df_new = df_new.join(df_ref, on='segment_grp_1', how='left')
    df_ref.unpersist()
    df_new = df_new.toDF(*[c.strip() for c in df_new.columns])  # Strip column names

    # Select columns present in old DataFrame and combine old and new DataFrames
    df_new = df_new.select(df.columns)
    df_all = df.union(df_new)
    df_new.unpersist()
    df.unpersist()

    # Filter rows where 'bundle' is not empty or null and drop duplicates based on 'bundle'
    df_all = df_all.filter((df_all['bundle'] != '') & df_all['bundle'].isNotNull()).dropDuplicates(['bundle'])

    # Define and apply filtering and grouping logic based on keywords
    # 1. Budget Manager
    budget_keywords = ['transaction', 'invoice', 'bill', 'trx', 'expenses', 'ratio', 'cash', 'loan', 'money',
                       'credit', 'calculator', 'bank', 'earn', 'tax', 'income', 'jobs', 'cv', 'resume']
    filter_condition = (
        (col('segment_grp') == 'Corporate Professional') &
        (col('segment_grp_1') == 'Corporate Professional') &
        (col('app_name').rlike('|'.join(budget_keywords)) | col('details').rlike('|'.join(budget_keywords)))
    )
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Budget Manager').otherwise(col('segment_grp_1')))

    # 2. Wealth Manager
    wealth_keywords = ['share', 'share price', 'euro', 'rupiah', 'yuan', 'ringgit', 'riyal', 'TAKA', 'invest',
                       'bit', 'bitcoin', 'coin', 'dollar', 'mining', 'gold', 'stock', 'market', 'exchange']
    filter_condition = (
        (col('segment_grp') == 'Corporate Professional') &
        (col('segment_grp_1') == 'Corporate Professional') &
        (col('app_name').rlike('|'.join(wealth_keywords)) | col('details').rlike('|'.join(wealth_keywords)))
    )
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Wealth Manager').otherwise(col('segment_grp_1')))

    # 3. General Corporate Professional if not Budget or Wealth Manager
    filter_condition = (
        (col('segment_grp') == 'Corporate Professional') &
        (col('segment_grp_1') != 'Wealth Manager') &
        (col('segment_grp_1') != 'Budget Manager')
    )
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Not Found').otherwise(col('segment_grp_1')))

    # 4. Passive Entertainer
    entertainment_keywords = ['entertainment', 'music', 'audio', 'video players']
    filter_condition = (
        (col('segment_grp') == 'Entertainment Lover') &
        (col('segment_grp_1') == 'Entertainment Lover') &
        (col('app_name').rlike('|'.join(entertainment_keywords)) | col('details').rlike('|'.join(entertainment_keywords)))
    )
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Passive Entertainer').otherwise(col('segment_grp_1')))

    # 5. Gamers if not Passive Entertainer
    filter_condition = (col('segment_grp') == 'Entertainment Lover') & (col('segment_grp_1') != 'Passive Entertainer')
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Gamers').otherwise(col('segment_grp_1')))

    # 6. Fitness Junkie
    fitness_keywords = ['workout', 'fitness', 'yoga', 'exercise', 'trainer', 'weight']
    filter_condition = (
        (col('segment_grp') == 'Health Junkie & Fitness Junkie') &
        (col('segment_grp_1') == 'Health Junkie & Fitness Junkie') &
        (col('app_name').rlike('|'.join(fitness_keywords)) | col('details').rlike('|'.join(fitness_keywords)))
    )
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Fitness Junkie').otherwise(col('segment_grp_1')))

    # 7. Phone Phreak
    phone_keywords = ['wallpaper', 'sticker', 'keyboard', 'theme', 'battery', 'screen']
    filter_condition = (
        (col('segment_grp') == 'Not Found') &
        (col('app_name').rlike('|'.join(phone_keywords)) | col('details').rlike('|'.join(phone_keywords)))
    )
    df_all = df_all.withColumn('segment_grp_1', when(filter_condition, 'Phone Phreak').otherwise(col('segment_grp_1')))

    # Save the final DataFrame to the specified path
    df_all.write.csv(old_result, mode='overwrite', header=True)

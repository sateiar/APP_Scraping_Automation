from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import date
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
    parser = argparse.ArgumentParser(description='Process app and persona data for categorization and segmentation.')
    
    parser.add_argument('--ref_app_path', required=True, type=str, 
                        help='Path to reference app data (e.g., s3a://ada-business-insights/data/reference/ref_intermediate/ref_app.csv)')
    parser.add_argument('--ref_persona_path', required=True, type=str, 
                        help='Path to reference persona rank data (e.g., s3a://ada-business-insights/data/reference/ref_intermediate/ref_persona_rank.csv)')
    parser.add_argument('--out_file', required=True, type=str, 
                        help='Path to app JSON output file (e.g., s3://ada-dev/app_category/ref/scrap_result/)')
    parser.add_argument('--result_path', required=True, type=str, 
                        help='Path to save result data (e.g., s3://ada-dev/app_category/ref/temp_result/)')
    parser.add_argument('--start_date', required=True, type=str, help='Start date in YYYYMMDD format')
    parser.add_argument('--end_date', required=True, type=str, help='End date in YYYYMMDD format')

    return parser

if __name__ == '__main__':
    # Initialize Spark session and argument parser
    spark = get_spark()
    parser = get_parser()
    today = date.today()
    args = parser.parse_args()

    # Parse command-line arguments
    ref_app_path = args.ref_app_path
    ref_persona_path = args.ref_persona_path
    out_file = args.out_file
    result_path = args.result_path
    start_date = args.start_date
    end_date = args.end_date

    # Set up the source date range for metadata in output data
    source_date = f"{start_date}-{end_date}"

    # Load reference data for apps and personas
    refapp_df = spark.read.csv(ref_app_path, header=True)
    refpersona_df = spark.read.csv(ref_persona_path, header=True)
    
    # Load app data from JSON file and process app details
    app_df = spark.read.json(out_file)
    app3_df = app_df.selectExpr(
        'bundle',
        'mapped_info.app_url',
        'mapped_info.appl_name AS name',
        'UPPER(mapped_info.app_cat) AS app_cat',
        'mapped_info.app_detail',
        'mapped_info.app_rank',
        'mapped_info.app_age_grp',
        'mapped_info.app_age_grp2',
        'unique_ifa'
    ).distinct()

    # Define a UDF to determine platform type based on bundle format
    udf_to_platform = F.udf(lambda x: 'IOS' if x.isdigit() else 'ANDROID')

    # Add metadata columns to app data for platform, source, and last update
    app_final_df = app3_df.withColumn('source', F.lit(source_date))\
                          .withColumn('last_update', F.lit(today.strftime('%d/%m/%Y')))\
                          .withColumn('platform', udf_to_platform('bundle'))

    # Perform left join between app data and reference app data to match categories
    join_df = app_final_df.join(
        refapp_df,
        app_final_df.app_cat == refapp_df.app_cat,
        'left'
    )

    # Select and rename columns as per the final schema requirements
    join1_df = join_df.selectExpr(
        'source', 'bundle', 'store_cat', 'category', '"" AS segment', 'segment_grp',
        'segment_grp AS segment_grp_1', '"" AS segment_grp_2', 'last_update', 'platform',
        'name AS app_name', 'app_detail AS details', 'app_url AS link', 'app_rank',
        'app_age_grp', 'app_age_grp2'
    )

    # Perform a left join with persona reference data to enrich with persona segment info
    join2_df = join1_df.join(
        refpersona_df,
        join1_df.segment_grp_1 == refpersona_df.ref_segment_grp_1,
        'left'
    )

    # Select final set of columns, including persona and app metadata
    join3_df = join2_df.select(
        'source', 'bundle', 'store_cat', 'category', 'segment', 'segment_grp', 
        'segment_grp_1', 'segment_grp_2', 'segment_index', 'last_update', 
        'platform', 'app_name', 'details', 'link', 'app_rank', 'app_age_grp', 'app_age_grp2'
    )

    # Calculate unique device counts per app bundle and join back to the main DataFrame
    cnt_df = app_final_df.groupby('bundle').agg(F.sum('unique_ifa').alias('ifa_cnt')).withColumnRenamed('bundle', 'ref_bundle')
    join4_df = join3_df.join(cnt_df, join3_df.bundle == cnt_df.ref_bundle, 'left').drop('ref_bundle')

    # Display distinct segment groups count for verification
    print("Distinct segment_grp_1 count:", join4_df.select('segment_grp_1').distinct().count())

    # Save the final DataFrame to the specified result path in Parquet format
    join4_df.write.parquet(result_path, mode="overwrite")

    # Stop the Spark session
    spark.stop()

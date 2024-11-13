from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import numpy as np
import time
from datetime import date, timedelta
from bs4 import BeautifulSoup
import json
from fake_useragent import UserAgent
import argparse

def get_spark():
    """
    Initializes and returns a Spark session.
    Sets the log level to ERROR to minimize log verbosity.
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
    parser = argparse.ArgumentParser(description='Process and categorize app data with scraping and Spark processing.')
    
    parser.add_argument('--result_path', required=True, type=str, 
                        help='Path for saving the result data (e.g., s3://ada-prod-data/etl/data/result/)')
    parser.add_argument('--app_list_file', required=True, type=str, 
                        help='Path to app list file before segmentation (e.g., s3://ada-business-insights/data/reference/personas_list.csv)')

    return parser

def requests_retry_session(retries=3, backoff_factor=10, status_forcelist=(500, 502, 504), session=None):
    """
    Configures and returns a requests session with retry logic for handling HTTP request failures.
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def get_random_ua():
    """
    Generates a random User-Agent string to avoid detection by web servers.
    """
    user_agent = UserAgent()
    return user_agent.random

def get_random_delay():
    """
    Generates a random delay between requests to reduce load on target servers.
    """
    delays = [2, 3, 4]
    return np.random.choice(delays)

# Functions for retrieving app category and information from various sources
def get_myappwhiz_category(app_id):
    return ('', '', '', 'Not Found', '', '', '')

def get_arkapk_category(app_id):
    """
    Retrieves app information from arkapk.com, including category, using BeautifulSoup for parsing.
    """
    app_url = f'https://arkapk.com/app/{app_id}'
    headers = {"user-agent": get_random_ua(), "Accept-Language": "en-US,en;q=0.5"}
    
    try:
        response = requests_retry_session().get(app_url, headers=headers)
        time.sleep(get_random_delay())
        soup = BeautifulSoup(response.text, "lxml")
        
        # Extract app details
        app_name = soup.find('h1', {'itemprop': 'name'}).text if soup.find('h1', {'itemprop': 'name'}) else ''
        app_cat = soup.select_one('meta[itemprop="applicationSubCategory"]') or \
                  soup.select_one('meta[itemprop="applicationCategory"]')
        app_cat = app_cat["content"].capitalize() if app_cat else ''
        
        return (app_url, app_name, '', app_cat, '', '', '')
    except:
        return ('', '', '', 'Not Found', '', '', '')

# Function for retrieving app information from iTunes API
def get_itunes(app_id):
    """
    Fetches app details from the iTunes API.
    """
    url = f"https://itunes.apple.com/lookup?id={app_id}"
    try:
        response = requests.get(url)
        time.sleep(get_random_delay())
        response.raise_for_status()
        data = response.json()

        if data.get('resultCount', 0) > 0:
            app_data = data['results'][0]
            return (
                app_data.get('trackViewUrl', ''),
                app_data.get('trackName', ''),
                app_data.get('description', ''),
                app_data.get('primaryGenreName', ''),
                app_data.get('averageUserRating', ''),
                app_data.get('ageRating', ''),
                app_data.get('contentAdvisoryRating', '')
            )
        else:
            return get_appleapp_category(app_id)
    except:
        return get_myappwhiz_category(app_id)

# Retrieve app category information from Google Play and Apple App Store
def get_googleapp_category(app_id):
    """
    Retrieves app details from Google Play Store using BeautifulSoup for HTML parsing.
    """
    app_url = f'https://play.google.com/store/apps/details?id={app_id}'
    headers = {"user-agent": get_random_ua(), "Accept-Language": "en-US,en;q=0.5"}
    
    try:
        response = requests_retry_session().get(app_url, headers=headers)
        time.sleep(get_random_delay())
        soup = BeautifulSoup(response.text, "lxml")
        
        # Extract app details
        app_name = soup.find('h1', {'itemprop': 'name'}).text if soup.find('h1', {'itemprop': 'name'}) else ''
        app_cat = soup.find('a', {'itemprop': 'genre'}).get('href').split('/')[-1].upper() if soup.find('a', {'itemprop': 'genre'}) else ''
        app_age_grp = soup.find_all('img', {'class': 'T75of E1GfKc'})[0]['alt'] if soup.find_all('img', {'class': 'T75of E1GfKc'}) else ''
        
        return (app_url, app_name, '', app_cat, '', app_age_grp, '')
    except:
        return get_arkapk_category(app_id)

if __name__ == '__main__':
    # Initialize Spark session and argument parser
    spark = get_spark()
    parser = get_parser()
    args = parser.parse_args()

    # Parse command-line arguments
    result_path = args.result_path
    app_list_file = args.app_list_file

    # Define UDF to get app category information based on app_id type
    udp_to_category = F.udf(lambda app_id: get_itunes(app_id) if app_id.isdigit() else get_googleapp_category(app_id), 
                            T.StructType([
                                T.StructField("app_url", T.StringType(), True),
                                T.StructField("appl_name", T.StringType(), True),
                                T.StructField("app_detail", T.StringType(), True),
                                T.StructField("app_cat", T.StringType(), True),
                                T.StructField("app_rank", T.StringType(), True),
                                T.StructField("app_age_grp", T.StringType(), True),
                                T.StructField("app_age_grp2", T.StringType(), True)
                            ]))

    # Load app list and filter based on conditions
    list_df = spark.read.csv(app_list_file, header=True)
    list_df = list_df.filter(F.col("unique_ifa") > 20).orderBy(F.desc("unique_ifa"))
    list_df = list_df.groupby('bundle').agg(F.sum('unique_ifa').alias('unique_ifa'))

    # Filter existing apps if result_path exists
    try:
        com_df = spark.read.json(result_path)
        list_df = list_df.join(com_df.selectExpr('bundle AS app_bundle').distinct(),
                               list_df.bundle == com_df.app_bundle, 'left').filter(F.col('app_bundle').isNull())
    except:
        pass

    # Group and iterate over data in chunks to apply UDF
    list_df = list_df.withColumn('RN', F.row_number().over(F.orderBy('bundle'))).withColumn('grp', F.ceil(F.col('RN') / 500))
    max_val = list_df.agg(F.max('grp')).first()[0]

    # Process each group, apply UDF, and save results
    for x in range(1, max_val + 1):
        temp_df = list_df.filter(F.col('grp') == x)
        temp_final_df = temp_df.withColumn('mapped_info', udp_to_category('bundle'))
        temp_final_df.write.json(result_path, mode='append')

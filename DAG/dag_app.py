# -*- coding: utf-8 -*-
# Copyright (c) 2019 AxiataADA
"""
This DAG performs daily aggregation of raw data and stores it in the data lake (S3).
"""

import os
import json
import yaml
import ast
from pathlib import Path
import airflow
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from datetime import datetime, timedelta

# =================================================================================================================================#
#                                                Configuration Setup                                                               #
# =================================================================================================================================#

# Determine environment configuration
env_name = os.environ['AIRFLOW_ENV_NAME']
ENV = 'prod' if env_name in ['mlai-ops-prod', 'staging'] else 'preprod' if env_name == 'ml-ops-preprod' else 'dev'
CONFIG_FILE = f'config_{ENV}.yaml'

# Load the DAG configuration from YAML file
config_path = Path(__file__).with_name(CONFIG_FILE)
with config_path.open() as f:
    config = yaml.safe_load(f)
dag_config = json.loads(json.dumps(config['dag'], default=str))

# MS Teams webhook connection
MS_TEAM_CONN = dag_config['default_args']['http_conn_id']

# Date macros
RUN_MONTH_ID = "{{ macros.ds_format(macros.ds_add(ds, -30),'%Y-%m-%d','%Y%m') }}"
RUN_YEAR     = "{{ macros.ds_format(macros.ds_add(ds, -30),'%Y-%m-%d','%Y') }}"
RUN_MONTH    = "{{ macros.ds_format(macros.ds_add(ds, -30),'%Y-%m-%d','%m') }}"

# EMR-related configuration
EMR_RELEASE = config['emr_release']
EMR_LOG_PATH = config['emr_log_path']
EMR_PACKAGES = config['emr_packages']
EMR_BOOTSTRAP = ast.literal_eval(config['emr_bootstrap'])
EMR_TAG = ast.literal_eval(config['emr_tag'])
JobFlowRole = config['JobFlowRole']
ServiceRole = config['ServiceRole']

# Athena configuration
ATHENA_DB = config['athena_database']
ATHENA_OUTPUT = config['athena_output']
ATHENA_WORKGROUP = config['athena_workgroup']
ATHENA_REF_PERSONA_V1 = config['table_persona_v1']
ATHENA_REF_PERSONA_V2 = config['table_persona_v2']

# Date variables
current_date = datetime.now()
current_year = current_date.year
current_month = current_date.month

# Last month's start and end dates
last_month = current_date.replace(day=1) - timedelta(days=1)
start_date = last_month.replace(day=1).strftime('%Y%m%d')
end_date = last_month.strftime('%Y%m%d')
date_string = f'{current_year}{current_month:02d}'

# Paths from configuration
input_agg = config['project_data']['input_agg']
app_list_file = config['project_data']['app_list_file']
app_list_file_not_found = config['project_data']['app_list_file_not_found'] + date_string + '/'
scrap_result = config['project_data']['scrap_result'] + date_string + '/'
ref_app_path = config['project_data']['ref_app_path']
ref_persona_path = config['project_data']['ref_persona_path']
temp_result = config['project_data']['temp_result'] + date_string + '/'

# S3 Paths for scripts
APP_LIST_SCRIPT = f"{config['S3_PATH_CODE']}app_checker.py"
SCRAP_SCRIPT = f"{config['S3_PATH_CODE']}scrap.py"
SEGMENT_SCRIPT = f"{config['S3_PATH_CODE']}app_segment.py"
CLEAN_SCRIPT = f"{config['S3_PATH_CODE']}data_cleaning_1.py"

# Instance configurations
instance_type = config['large']['instance_type']
cluster_config = config['large']['cluster_config']
driver_memory = config['large']['instance_setting']['driver-memory']
driver_core = config['large']['instance_setting']['driver-cores']
executor_memory = config['large']['instance_setting']['executor-memory']
executor_core = config['large']['instance_setting']['executor-cores']
num_executors = config['large']['instance_setting']['num-executors']

# =================================================================================================================================#
#                                                Default Arguments                                                                 #
# =================================================================================================================================#

default_args = {
    'owner': 'ml-model',
    'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

# =================================================================================================================================#
#                                                EMR Pipeline DAG                                                                  #
# =================================================================================================================================#

with DAG(**dag_config) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    random_ua_generate = DummyOperator(task_id='Random_UA_Generate')

    # Step 1: Generate App List
    app_list_generate_step = [
        {
            'Name': f'app_list_generate_{current_year}{current_month:02d}',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
                    '--deploy-mode', 'client',
                    '--packages', EMR_PACKAGES,
                    '--driver-memory', driver_memory,
                    '--driver-cores', str(driver_core),
                    '--num-executors', str(num_executors),
                    '--executor-memory', executor_memory,
                    '--executor-cores', str(executor_core),
                    '--conf', 'spark.driver.maxResultSize=0',
                    '--conf', 'spark.yarn.maxAppAttempts=1',
                    '--py-files', ','.join([APP_LIST_SCRIPT]),
                    APP_LIST_SCRIPT,
                    '--data_path', input_agg,
                    '--app_list_file', app_list_file,
                    '--start_date', start_date,
                    '--end_date', end_date,
                    '--result_path', app_list_file_not_found
                ]
            }
        }
    ]

    # Cluster creation and monitoring for App List Generation
    app_list_generate_job = {
        'Name': f'app_list_generate_{current_year}{current_month:02d}',
        'ReleaseLabel': EMR_RELEASE,
        "Applications": [{"Name": "Spark"}, {"Name": "Ganglia"}],
        'LogUri': EMR_LOG_PATH,
        'Steps': app_list_generate_step,
        'Instances': instance_type,
        'BootstrapActions': EMR_BOOTSTRAP,
        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
        'Configurations': cluster_config,
        'JobFlowRole': 'EMR_EC2_AutomationRole',
        'ServiceRole': 'EMR_AutomationServiceRole',
        'Tags': EMR_TAG
    }

    app_list_generate_cluster_creator = EmrCreateJobFlowOperator(
        task_id=f'app_list_generate_{current_year}{current_month:02d}',
        job_flow_overrides=app_list_generate_job
    )
    app_list_generate_job_sensor = EmrJobFlowSensor(
        task_id=f'watch_app_list_generate_{current_year}{current_month:02d}',
        job_flow_id=f"{{{{ task_instance.xcom_pull('app_list_generate_{current_year}{current_month:02d}', key='return_value') }}}}"
    )

    # Step 2: Scraping
    app_scraping_step = [
        {
            'Name': f'app_scraping_{current_year}{current_month:02d}',
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit',
                    '--deploy-mode', 'client',
                    '--packages', EMR_PACKAGES,
                    '--conf', 'spark.driver.maxResultSize=0',
                    '--conf', 'spark.yarn.maxAppAttempts=1',
                    '--py-files', ','.join([APP_LIST_SCRIPT]),
                    SCRAP_SCRIPT,
                    '--result_path', scrap_result,
                    '--app_list_file', app_list_file_not_found
                ]
            }
        }
    ]

    # Cluster creation and monitoring for Scraping
    app_scraping_job = {
        'Name': f'app_scraping_{current_year}{current_month:02d}',
        'ReleaseLabel': EMR_RELEASE,
        "Applications": [{"Name": "Spark"}, {"Name": "Ganglia"}],
        'LogUri': EMR_LOG_PATH,
        'Steps': app_scraping_step,
        'Instances': instance_type,
        'BootstrapActions': EMR_BOOTSTRAP,
        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
        'Configurations': cluster_config,
        'JobFlowRole': 'EMR_EC2_AutomationRole',
        'ServiceRole': 'EMR_AutomationServiceRole',
        'Tags': EMR_TAG
    }

    app_scraping_cluster_creator = EmrCreateJobFlowOperator(
        task_id=f'app_scraping_{current_year}{current_month:02d}',
        job_flow_overrides=app_scraping_job
    )
    app_scraping_job_sensor = EmrJobFlowSensor(
        task_id=f'watch_app_scraping_{current_year}{current_month:02d}',
        job_flow_id=f"{{{{ task_instance.xcom_pull('app_scraping_{current_year}{current_month:02d}', key='return_value') }}}}"
    )
    

    #segment    
    app_segment_step = [
            # Step 1 - data generation
            {
            'Name': 'app_segment_{}{:02d}'.format(current_year,current_month),
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit', 
                    '--deploy-mode','client',
                    '--packages', EMR_PACKAGES,
                    '--conf' , 'spark.driver.maxResultSize=0',
                    '--conf','spark.yarn.maxAppAttempts=1',
                    '--py-files' , ','.join([APP_LIST_SCRIPT]), 
                    SEGMENT_SCRIPT,
                    '--ref_app_path' ,ref_app_path , 
                    '--ref_persona_path' , ref_persona_path, 
                    '--out_file', scrap_result,
                    '--result_path', temp_result, 
                    '--start_date', start_date,
                    '--end_date', end_date
                    ]
                }
            }
        ]


    app_segment_job = {
        'Name': 'app_segment_{}{:02d}'.format(current_year,current_month),
        'ReleaseLabel': EMR_RELEASE,
        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
        'LogUri': EMR_LOG_PATH,
        'Steps': app_segment_step,
        'Instances': instance_type,
        'BootstrapActions': EMR_BOOTSTRAP,
        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
        'Configurations': cluster_config, 
        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
        # 'ServiceRole': 'EMR_AutomationServiceRole',
        'JobFlowRole': 'EMR_EC2_AutomationRole',
        'ServiceRole': 'EMR_AutomationServiceRole',
        # 'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
        'Tags': EMR_TAG
    }

    app_segment_cluster_creator = EmrCreateJobFlowOperator(
        task_id='app_segment_{}{:02d}'.format(current_year,current_month),
        job_flow_overrides= app_segment_job,
        #on_failure_callback=on_failure
    )
    app_segment_job_sensor = EmrJobFlowSensor(
        task_id='watch_app_segment_{}{:02d}'.format(current_year,current_month),
        job_flow_id="{{{{ task_instance.xcom_pull('app_segment_{}{:02d}', key='return_value') }}}}".format(current_year,current_month),

        #on_failure_callback=on_failure
        )



    #clean    
    app_clean_step = [
            # Step 1 - data generation
            {
            'Name': 'app_clean_{}{:02d}'.format(current_year,current_month),
            'ActionOnFailure': 'TERMINATE_CLUSTER',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    '/usr/bin/spark-submit', 
                    '--deploy-mode','client',
                    '--packages', EMR_PACKAGES,
                    # '--driver-memory', driver_memory
                    # '--driver-cores', str(driver_core),
                    # '--num-executors' , str(num_executors),
                    # '--executor-memory', executor_memory,
                    # '--executor-cores' , str(executor_core),
                    '--conf' , 'spark.driver.maxResultSize=0',
                    '--conf','spark.yarn.maxAppAttempts=1',
                    # '--conf','spark.sql.shuffle.partitions=500',
                    '--py-files' , ','.join([APP_LIST_SCRIPT]), 
                    CLEAN_SCRIPT,
                    '--old_result' ,app_list_file , 
                    '--new_result' , temp_result, 
                    ]
                }
            }
        ]


    app_clean_job = {
        'Name': 'app_clean_{}{:02d}'.format(current_year,current_month),
        'ReleaseLabel': EMR_RELEASE,
        "Applications": [{"Name": "Spark"},{"Name": "Ganglia"}],
        'LogUri': EMR_LOG_PATH,
        'Steps': app_clean_step,
        'Instances': instance_type,
        'BootstrapActions': EMR_BOOTSTRAP,
        'ScaleDownBehavior': 'TERMINATE_AT_TASK_COMPLETION',
        'Configurations': cluster_config, 
        # 'JobFlowRole': 'EMR_EC2_AutomationRole',
        # 'ServiceRole': 'EMR_AutomationServiceRole',
        'JobFlowRole': 'EMR_EC2_AutomationRole',
        'ServiceRole': 'EMR_AutomationServiceRole',
        # 'AutoScalingRole': 'EMR_AutoScaling_DefaultRole',
        'Tags': EMR_TAG
    }

    app_clean_cluster_creator = EmrCreateJobFlowOperator(
        task_id='app_clean_{}{:02d}'.format(current_year,current_month),
        job_flow_overrides= app_clean_job,
        #on_failure_callback=on_failure
    )
    app_clean_job_sensor = EmrJobFlowSensor(
        task_id='watch_app_clean_{}{:02d}'.format(current_year,current_month),
        job_flow_id="{{{{ task_instance.xcom_pull('app_clean_{}{:02d}', key='return_value') }}}}".format(current_year,current_month),

        #on_failure_callback=on_failure
        )
    
    drop_table_query1 = """DROP TABLE IF EXISTS {ATHENA_DB}.{ATHENA_REF_PERSONA_V1} """
    drop_table_task1 = AWSAthenaOperator(
        task_id='drop_table_personas_v1',
        query= drop_table_query1.format(ATHENA_DB=ATHENA_DB, ATHENA_REF_PERSONA_V1=ATHENA_REF_PERSONA_V1),
        database = ATHENA_DB,
        output_location = ATHENA_OUTPUT,
        workgroup = ATHENA_WORKGROUP,
        dag=dag
    )




    update_table_query = """CREATE EXTERNAL TABLE IF NOT EXISTS {ATHENA_DB}.{ATHENA_REF_PERSONA_V1} (
                            source string,
                            bundle string,
                            store_cat string,
                            category string,
                            segment_grp string,
                            segment_grp_1 string,
                            segment_index string,
                            last_update string,
                            platform string,
                            app_name string,
                            link string,
                            app_rank string,
                            ifa_cnt string,
                            app_age_grp string,
                            app_age_grp2 string
                            )
                            ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
                            STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
                            OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
                            LOCATION '{app_list_file}';
                        """
    create_table_task1 = AWSAthenaOperator(
        task_id='add_data_to_table',
        query= update_table_query.format(ATHENA_DB=ATHENA_DB, ATHENA_REF_PERSONA_V1=ATHENA_REF_PERSONA_V1, app_list_file=app_list_file),
        database = ATHENA_DB,
        output_location = ATHENA_OUTPUT,
        workgroup = ATHENA_WORKGROUP,
        dag=dag
    )




    query3 = """ DROP VIEW {ATHENA_DB}.{ATHENA_REF_PERSONA_V2}"""
    drop_table_task2 = AWSAthenaOperator(
        task_id='drop_table_persona_V2',
        query= query3.format(ATHENA_DB = ATHENA_DB ,ATHENA_REF_PERSONA_V2=ATHENA_REF_PERSONA_V2 ) ,
        database = ATHENA_DB,
        output_location = ATHENA_OUTPUT,
        workgroup = ATHENA_WORKGROUP,
        dag=dag
    )

    query2  = """ Create view {ATHENA_DB}.{ATHENA_REF_PERSONA_V2} as (
                    Select *,
                        case
                            when segment_grp_1 in ('Fitness Junkie' , 'Health and Fitness Junkie') then 'Workout Warrior'
                            when segment_grp_1 in ('Health Junkie', 'Heealth Junkie') then 'Health Fanatics'
                            when segment_grp_1 in ('Wealth Manager') then 'Wealth Manager'
                            when segment_grp_1 in ('Bookworm') then 'Bookworm'
                            when segment_grp_1 in ('Budget Manager') then 'Budget Managers'
                            when segment_grp_1 in ('Passive Entertainer') then 'Entertainment Junkies'
                            when segment_grp_1 in ('Social Butterfly') then 'Social Butterflies'
                            when segment_grp_1 in ('Creative Crowd') then 'Creative Crowd'
                            when segment_grp_1 in ('Phone Phreak') then 'Phone Enthusiast'
                            when segment_grp_1 in ('Gamers') then 'Gamers'
                            when segment_grp_1 is null
                            or segment_grp_1 in (null, '') then 'Not Found' else segment_grp_1
                        end as persona,
                        case
                            when segment_grp_1 in ('Fitness Junkie') then 1.0
                            when segment_grp_1 in ('Health Junkie', 'Heealth Junkie') then 2.0
                            when segment_grp_1 in ('Wealth Manager') then 3.0
                            when segment_grp_1 in ('Bookworm') then 4.0
                            when segment_grp_1 in ('Budget Manager') then 5.0
                            when segment_grp_1 in ('Passive Entertainer') then 6.0
                            when segment_grp_1 in ('Social Butterfly') then 7.0
                            when segment_grp_1 in ('Creative Crowd') then 8.0
                            when segment_grp_1 in ('Phone Phreak') then 9.0
                            when segment_grp_1 in ('Gamers') then 10.0
                        end as rank,
                        CASE
                            WHEN c.category = 'Art & Design' THEN 'Art, Graphics & Design'
                            WHEN c.category = 'Auto & Vehicles' THEN 'Auto & Vehicles'
                            WHEN c.category = 'Beauty' THEN 'Health & Fitness'
                            WHEN c.category like 'Book%' THEN 'Books and Comics'
                            WHEN c.category = 'Business' THEN 'Business'
                            WHEN c.category = 'Comics' THEN 'Books and Comics'
                            WHEN c.category = 'Communication' THEN 'Social'
                            WHEN c.category = 'Dating' THEN 'Social'
                            WHEN c.category = 'Dictionary' THEN 'Education and Training'
                            WHEN c.category = 'Education' THEN 'Education and Training'
                            WHEN c.category = 'Entertainment' THEN 'Entertainment'
                            WHEN c.category = 'Events' THEN 'Social'
                            WHEN c.category = 'Finance' THEN 'Finance'
                            WHEN c.category = 'Food & Drink' THEN 'Food & Drink'
                            WHEN c.category like 'Game%' THEN 'Games'
                            WHEN c.category = 'Graphics & Design' THEN 'Art, Graphics & Design'
                            WHEN c.category = 'Health & Fitness' THEN 'Health & Fitness'
                            WHEN c.category = 'House & Home' THEN 'House & Home'
                            WHEN c.category = 'Libraries & Demo' THEN 'Education and Training'
                            WHEN c.category like 'Lifestyle%' THEN 'Lifestyle'
                            WHEN c.category = 'Maps & Navigation' THEN 'Travel'
                            WHEN c.category = 'Medical' THEN 'Medical'
                            WHEN c.category like 'Music%' THEN 'Music'
                            WHEN c.category = 'Navigation' THEN 'Travel'
                            WHEN c.category like 'News%' THEN 'News & Magazines'
                            WHEN c.category = 'Parenting' THEN 'Parenting'
                            WHEN c.category like '%Personalization%' THEN 'Personalization'
                            WHEN c.category like 'Photo%' THEN 'Photo, Video & Editing'
                            WHEN c.category like 'Productivity%' THEN 'Office & Productivity'
                            WHEN c.category = 'Quote' THEN 'Education and Training'
                            WHEN c.category = 'Reference' THEN 'Education and Training'
                            WHEN c.category = 'Religious' THEN 'Religious'
                            WHEN c.category = 'Shopping' THEN 'Shopping'
                            WHEN c.category = 'Simulation' THEN 'Games'
                            WHEN c.category like 'Social%' THEN 'Social'
                            WHEN c.category = 'Sports' THEN 'Sports'
                            WHEN c.category in (
                                'Tools',
                                'Tools:Calculator',
                                'Tools:Clock',
                                'Tools:Developer',
                                'Tools:Flashlight',
                                'Tools:Remote'
                            ) THEN 'Tools & Utilities'
                            WHEN c.category in (
                                'Tools:Battery',
                                'Tools:Booster',
                                'Tools:Cleaner',
                                'Tools:Scanner',
                                'Tools:Security',
                                'Tools:SpeedTest'
                            ) THEN 'Mobile Performance'
                            WHEN c.category in (
                                'Tools:File Manager',
                                'Tools:File Share'
                            ) THEN 'Office & Productivity'
                            when c.category = 'Tools:Browser' THEN 'Mobile Performance'
                            WHEN c.category = 'Tools:Downloader' THEN 'Photo, Video & Editing'
                            WHEN c.category = 'Tools:Translator' THEN 'Translation'
                            WHEN c.category = 'Tools:VPN' THEN 'Mobile Performance'
                            WHEN c.category like 'Travel%' THEN 'Travel'
                            WHEN c.category = 'Utilities' THEN 'Tools & Utilities'
                            WHEN c.category like 'Video%' THEN 'Photo, Video & Editing'
                            WHEN c.category = 'Weather' THEN 'Tools & Utilities'
                            when c.category is null
                            or c.category in (null, '', 'Not Found') then 'Not Found'
                        END as app_category
                    From "{ATHENA_REF_PERSONA_V1}" c
                    Union
                    Select *,
                        case
                            when segment_grp_1 in ('Fitness Junkie') then 'Workout Warrior'
                            when segment_grp_1 in ('Health Junkie', 'Heealth Junkie') then 'Health Fanatics'
                            when segment_grp_1 in ('Wealth Manager') then 'Wealth Manager'
                            when segment_grp_1 in ('Bookworm') then 'Bookworm'
                            when segment_grp_1 in ('Budget Manager') then 'Budget Managers'
                            when segment_grp_1 in ('Passive Entertainer') then 'Entertainment Junkies'
                            when segment_grp_1 in ('Social Butterfly') then 'Social Butterflies'
                            when segment_grp_1 in ('Creative Crowd') then 'Creative Crowd'
                            when segment_grp_1 in ('Phone Phreak') then 'Phone Enthusiast'
                            when segment_grp_1 in ('Gamers') then 'Gamers'
                            when segment_grp_1 is null
                            or segment_grp_1 in (null, '') then 'Not Found' else segment_grp_1
                        end as persona,
                        case
                            when segment_grp_1 in ('Fitness Junkie') then 1.0
                            when segment_grp_1 in ('Health Junkie', 'Heealth Junkie') then 2.0
                            when segment_grp_1 in ('Wealth Manager') then 3.0
                            when segment_grp_1 in ('Bookworm') then 4.0
                            when segment_grp_1 in ('Budget Manager') then 5.0
                            when segment_grp_1 in ('Passive Entertainer') then 6.0
                            when segment_grp_1 in ('Social Butterfly') then 7.0
                            when segment_grp_1 in ('Creative Crowd') then 8.0
                            when segment_grp_1 in ('Phone Phreak') then 9.0
                            when segment_grp_1 in ('Gamers') then 10.0
                        end as rank,
                        CASE
                            WHEN c.category = 'Beauty' THEN 'Beauty'
                            WHEN c.category = 'Communication' THEN 'Communication'
                            WHEN c.category = 'Events' THEN 'Lifestyle'
                            WHEN c.category in ('Games:Puzzle', 'Games:Trivia', 'Games:Word') THEN ('Games')
                            WHEN c.category = 'House & Home' THEN 'Lifestyle'
                            WHEN c.category = 'Music:Downloader' THEN 'Mobile Performance'
                            WHEN c.category = 'Shopping' THEN 'Lifestyle'
                            WHEN c.category = 'Sports' THEN 'Lifestyle'
                            WHEN c.category = 'Tools:Downloader' THEN 'Mobile Performance'
                            WHEN c.category = 'Video:Downloader' THEN 'Mobile Performance'
                        END AS app_category
                    From "{ATHENA_REF_PERSONA_V1}" c
                    Where c.category in (
                            'Games:Puzzle',
                            'Games:Trivia',
                            'Games:Word',
                            'Beauty',
                            'Communication',
                            'Events',
                            'House & Home',
                            'Music:Downloader',
                            'Shopping',
                            'Sports',
                            'Tools:Downloader',
                            'Tools:Scanner',
                            'Tools:Security',
                            'Video:Downloader'
                        )
                )"""
    create_table_task2 = AWSAthenaOperator(
        task_id='create_table_persona_V2',
        query= query2.format(ATHENA_DB = ATHENA_DB ,ATHENA_REF_PERSONA_V2=ATHENA_REF_PERSONA_V2,ATHENA_REF_PERSONA_V1 =ATHENA_REF_PERSONA_V1 ) ,
        database = ATHENA_DB,
        output_location = ATHENA_OUTPUT,
        workgroup = ATHENA_WORKGROUP,
        dag=dag
    )
    
    # start >> app_convert_cluster_creator >> app_convert_job_sensor >> end 
    start >> app_list_generate_cluster_creator >>  app_list_generate_job_sensor >> app_scraping_cluster_creator >>  app_scraping_job_sensor >> app_segment_cluster_creator >> app_segment_job_sensor >> app_clean_cluster_creator >> app_clean_job_sensor >>drop_table_task1 >> create_table_task1 >> drop_table_task2 >> create_table_task2 >> end  
    start >> app_list_generate_cluster_creator >>  app_list_generate_job_sensor >> Random_UA_Generate >> app_segment_cluster_creator >> app_segment_job_sensor >> app_clean_cluster_creator >> app_clean_job_sensor >> drop_table_task1 >> create_table_task1>> drop_table_task2 >> create_table_task2 >>  end 

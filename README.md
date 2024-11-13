# App Scraping and Categorization Automation Pipeline

This repository contains an Apache Airflow DAG for the daily aggregation, categorization, and segmentation of app data. The pipeline is configured to run on Amazon EMR, with processing results stored in an Amazon S3 data lake. Additional processes, including web scraping and data transformation, are integrated into the DAG.

## Table of Contents
- [Project Overview](#project-overview)
- [Pipeline Structure](#pipeline-structure)
- [Setup Instructions](#setup-instructions)
- [Configuration](#configuration)
- [requirements.txt](#requirementstxt)
- [Running the Pipeline](#running-the-pipeline)


## Project Overview
The Airflow DAG defined in this repository:
- **Aggregates** raw app data.
- **Categorizes** and **segments** apps based on specified rules.
- **Stores** results in Amazon S3 and Athena for easy querying and analysis.

The workflow leverages EMR for scalable data processing and utilizes Athena for data querying and table management.

## Pipeline Structure
The main steps in the pipeline are as follows:
1. **App List Generation** - Aggregates and generates an app list.
2. **Web Scraping** - Gathers additional app metadata.
3. **Segmentation** - Segments the app data based on predefined categories.
4. **Data Cleaning** - Cleans and finalizes the processed data.

Each task is implemented as an EMR job managed by Airflow’s `EmrCreateJobFlowOperator` and monitored with `EmrJobFlowSensor`.

## Setup Instructions

### Prerequisites
- Python 3.8+
- Apache Airflow 2.5+
- AWS account with permissions to create and manage EMR clusters, S3 buckets, and Athena tables.

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/app-categorization-pipeline.git
   cd app-categorization-pipeline
2. **Install dependencies: Make sure you are in a virtual environment, then install the required packages:

pip install -r requirements.txt
3. **Set up Airflow connections: In Airflow, configure the following:

  AWS Connection: Configure an AWS connection for accessing EMR, S3, and Athena.
  MS Teams Connection: If applicable, set up the connection for MS Teams notifications.

Configuration
Environment Variables: Set the AIRFLOW_ENV_NAME environment variable for your environment (prod, preprod, dev).

Configuration File:

Edit the config_<ENV>.yaml file to update S3 paths, EMR configurations, and Athena database information.
Example YAML Configuration:
dag:
  dag_id: app-categorization-full-cycle
  schedule_interval: '0 2 12 * *'
  tags: ['app', 'categorization', 'segmentation']
  catchup: false
  default_args:
    owner: airflow
    start_date: '2023-07-10T00:00:00'
emr_release: 'emr-5.31.0'
# Additional configurations here...
requirements.txt
# Airflow core and specific providers
apache-airflow==2.5.1
apache-airflow-providers-amazon==3.3.0

# YAML parsing and JSON handling
PyYAML==6.0
json5==0.9.6

# AWS and Spark-related packages
boto3==1.26.28
pyspark==3.3.0

# Web scraping dependencies
beautifulsoup4==4.11.1
requests==2.28.1
fake-useragent==0.1.11

# Other essential packages
pathlib==1.0.1
Running the Pipeline
Start Airflow:

airflow standalone
Trigger the DAG:

Go to the Airflow UI (typically at http://localhost:8080).
Enable and trigger the app-categorization-full-cycle DAG.
Monitor the DAG:

Check each step in the Airflow UI to view logs and monitor the job flow.

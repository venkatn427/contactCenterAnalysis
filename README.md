# Contact Center Analytics with Apache Spark on Databricks

This project is designed to process and analyze contact center data using Apache Spark on Databricks. The goal is to ingest, clean, transform, and analyze data to provide valuable insights for both agents and supervisors.

## Project Structure

- **main.py**: Main script to run the data processing and analytics pipeline.
- **utils.py**: Utility functions for reading and writing data.
- **schema.py**: Schema definitions for the data.
- **constant.py**: Constants used in the project.

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/venkatn427/contactCenterAnalysis.git
    cd contactCenterAnalysis
    ```

2. Install the required packages:
    ```bash
    pip install -r requirements.txt
    ```

## Usage
   Run main script 
   ```python
      python main.py
   ```
## Functions 
   1. Data Ingestion: Reads CSV files and writes them to Parquet.
   2. Data Cleaning and Transformation: Deduplicates and cleans data.
   3. Data Enrichment: Joins dataframes and enriches the data.
   4. Reporting and Analytics: Creates dashboard aggregates and team performance reports.
   5. Optimization: Writes data using Snappy compression and partitions data for efficient reads.
   6. Real-time Updates: Sets up Spark streaming for near real-time updates.
    
## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Data Ingestion](#data-ingestion)
4. [Data Cleaning and Transformation](#data-cleaning-and-transformation)
5. [Data Enrichment](#data-enrichment)
6. [Reporting and Analytics](#reporting-and-analytics)
7. [Optimization](#optimization)

## Introduction

The project processes data from CSV files stored in Azure Data Lake Storage (ADLS) and stores the results in Parquet format for faster read and write operations. It includes steps for data cleaning, transformation, and enrichment, followed by generating analytics dashboards for agents and supervisors.

## Getting Started

### Prerequisites

- Databricks workspace
- Azure Data Lake Storage (ADLS)
- Databricks cluster with Spark
      
### Installation

1. **Create a Databricks Cluster**:
   - Go to your Databricks workspace.
   - Create a new cluster with the desired configuration.

2. **Set up your Azure Data Lake Storage (ADLS)**:
   - Ensure your ADLS account and container are correctly configured.

3. **Mount ADLS to Databricks**:
   - Follow the Databricks documentation to mount your ADLS to the Databricks filesystem.

## Data Ingestion

1. **Initialize Spark Session**:
    ```python
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType
    from pyspark.sql.functions import unix_timestamp, when, count, avg, sum

    spark = SparkSession.builder.appName("ContactCenterAnalytics").getOrCreate()
    ```

2. **Define File Paths**:
    ```python
    base_path = "/abfss:/<container_name>/contact-center-analytics/"
    interactions_path = base_path + "interactions.csv"
    agents_path = base_path + "agents.csv"
    supervisors_path = base_path + "supervisors.csv"
    
    interactions_parquet_path = base_path + "interactions.parquet"
    agents_parquet_path = base_path + "agents.parquet"
    supervisors_parquet_path = base_path + "supervisors.parquet"

    optimized_base_path = base_path + "optimized_data/"
    interactions_enriched_path = optimized_base_path + "interactions_enriched.parquet"
    agents_cleaned_path = optimized_base_path + "agents_cleaned.parquet"
    supervisors_cleaned_path = optimized_base_path + "supervisors_cleaned.parquet"

    interactions_partitioned_path = optimized_base_path + "interactions_enriched_partitioned.parquet"
    agents_partitioned_path = optimized_base_path + "agents_cleaned_partitioned.parquet"
    supervisors_partitioned_path = optimized_base_path + "supervisors_cleaned_partitioned.parquet"

    detailed_reports_df_parquet_path = optimized_base_path + "detailed_reports_df.parquet"
    ```

4. **Define Schemas for the Files**:
    ```python
    interactions_schema = StructType([
        StructField("interaction_id", StringType(), True),
        StructField("agent_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("issue_type", StringType(), True),
        StructField("resolution", StringType(), True)
    ])

    agents_schema = StructType([
        StructField("agent_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("team", StringType(), True),
        StructField("hire_date", TimestampType(), True)
    ])

    supervisors_schema = StructType([
        StructField("supervisor_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("team", StringType(), True),
        StructField("hire_date", TimestampType(), True)
    ])
    ```

5. **Read Data from CSV Files**:
    ```python
    interactions_df = spark.read.csv(interactions_path, schema=interactions_schema, header=True)
    agents_df = spark.read.csv(agents_path, schema=agents_schema, header=True)
    supervisors_df = spark.read.csv(supervisors_path, schema=supervisors_schema, header=True)
    ```

6. **Save DataFrames as Parquet**:
    ```python
    interactions_df.write.parquet(interactions_parquet_path, mode='overwrite')
    agents_df.write.parquet(agents_parquet_path, mode='overwrite')
    supervisors_df.write.parquet(supervisors_parquet_path, mode='overwrite')
    ```
7. **Read from Parquet files**
   ```python
   interactions_df_parquet = spark.read.parquet(interactions_parquet_path)
   agents_df_parquet = spark.read.parquet(agents_parquet_path)
   supervisors_df_parquet = spark.read.parquet(supervisors_parquet_path)
   
## Data Cleaning and Transformation

1. **Remove Duplicates**:
      1. *Assuming we have a interaction id for each call or chat that's initited by customer.*
    ```python
    interactions_df_dedup = interactions_df.dropDuplicates(['interaction_id'])
    agents_df_dedup = agents_df.dropDuplicates(['agent_id'])
    supervisors_df_dedup = supervisors_df.dropDuplicates(['supervisor_id'])
    ```

3. **Handle Missing Values**:
      1. *All Id columns (agent_id, interaction_id, supervisor_id) are mandatory within the file and no missing values.*
      2. *Adding a default name as Unknown if name is missing in the dataset considering an agent is always part of one team and team is not empty.*
    ```python
    interactions_df_cleaned = interactions_df_dedup.na.drop(subset=['interaction_id', 'agent_id'])
    agents_df_cleaned = agents_df_dedup.na.fill({"name": "Unknown"})
    supervisors_df_cleaned = supervisors_df_dedup.na.fill({"name": "Unknown"})
    ```

## Data Enrichment

1. **Join with Agents and Supervisors**:
   1. *Assuming one supervisor for one team. to get all agents along with supervisors we use left join on interactions table*
   2. *Full joins are used to retain all records from both tables, useful for comprehensive data analysis*
   3. *Left joins are efficient when we want to keep all records from the left table and match the records from the right table*

 
    ```python
    # Join interactions with agents
    interactions_enriched_df = interactions_df_cleaned.join(agents_df_cleaned, on='agent_id', how='left')

    # Join with supervisors based on the team
    interactions_enriched_df = interactions_enriched_df.join(supervisors_df_cleaned, 
                                                         interactions_enriched_df.team == supervisors_df_cleaned.team,
                                                         how='left') \
                                                   .select("interaction_id", "agent_id", "customer_id", "start_time", "end_time", 
                                                           "issue_type", "resolution", "name", "team", 
                                                           "supervisor_id", "supervisors_df_cleaned.name", "supervisors_df_cleaned.hire_date") \
                                                   .withColumnRenamed("name", "agent_name") \
                                                   .withColumnRenamed("supervisors_df_cleaned.name", "supervisor_name")


3. **Add New Columns**:
    ```python
    # Calculate interaction duration
    interactions_enriched_df = interactions_enriched_df.withColumn(
        "interaction_duration",
        (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60  # duration in minutes
    )

    # Determine interaction resolution status
    interactions_enriched_df = interactions_enriched_df.withColumn(
        "resolution_status",
        when(interactions_enriched_df.resolution == 'Resolved', 'Resolved').otherwise('Not Resolved')
    )
    ```

## Reporting and Analytics

1. **Agent Dashboard - Insights by Agent**:
    ```python
    dashboard_aggregates_df = interactions_enriched_df.groupBy("agent_id").agg(
        count("interaction_id").alias("num_interactions"),
        avg("interaction_duration").alias("avg_interaction_duration"),
        sum(when(interactions_enriched_df.resolution_status == 'Resolved', 1).otherwise(0)).alias("resolved_interactions"),
        count("interaction_id").alias("total_interactions")
    )

    # Compute resolution rate
    dashboard_aggregates_df = dashboard_aggregates_df.withColumn(
        "resolution_rate", 
        dashboard_aggregates_df.resolved_interactions / dashboard_aggregates_df.total_interactions
    )

    # Cache the result
    dashboard_aggregates_df.cache()
    ```

2. **Near Real-Time Updates for Agent Dashboard**:
    ```python
    from pyspark.sql.streaming import StreamingQuery

    query = dashboard_aggregates_df.writeStream \
        .format("memory") \
        .queryName("agent_dashboard") \
        .outputMode("complete") \
        .start()

    def get_dashboard_data():
        return spark.sql("SELECT * FROM agent_dashboard")

    import time

    while True:
        dashboard_data = get_dashboard_data()
        dashboard_data.show()
        time.sleep(10)
    ```

3. **Supervisor Dashboard - Insights by Team**:
    ```python
    team_performance_df = interactions_enriched_df.groupBy("team").agg(
        count("interaction_id").alias("num_interactions"),
        avg("interaction_duration").alias("avg_interaction_duration"),
        sum(when(col("resolution_status") == 'Resolved', 1).otherwise(0)).alias("resolved_interactions"),
        count("interaction_id").alias("total_interactions")
    )

    team_performance_df = team_performance_df.withColumn(
        "resolution_rate", 
        team_performance_df.resolved_interactions / team_performance_df.total_interactions
    )

    detailed_reports_df = team_performance_df.join(supervisors_df_cleaned, on='team', how='left')
    detailed_reports_df.write.parquet(detailed_reports_df_parquet_path, mode='overwrite')
    ```

## Optimization

1. **Use Snappy Compression for Storage and Read Efficiency**:
    ```python
    interactions_enriched_df.write.parquet(interactions_enriched_path, mode='overwrite', compression='snappy')
    agents_df_cleaned.write.parquet(agents_cleaned_path, mode='overwrite', compression='snappy')
    supervisors_df_cleaned.write.parquet(supervisors_cleaned_path, mode='overwrite', compression='snappy')
    ```

2. **Data Partitioning for Efficient Read by Team and Agent ID**:
    ```python
    interactions_enriched_df.write.partitionBy("agent_id").parquet(interactions_partitioned_path, mode='overwrite', compression='snappy')
    agents_df_cleaned.write.partitionBy("team").parquet(agents_partitioned_path, mode='overwrite', compression='snappy')
    supervisors_df_cleaned.write.partitionBy("team").parquet(supervisors_partitioned_path, mode

3. **Using Broadcast Join while joining agents with Interactions Dataset**:
   1. *Assuming Agents and Supervisors Datasets is smaller compared to Interactions and can fit in memory*
   2. *Broadcast joins can be more efficient than regular joins because they avoid the expensive shuffle operation that normally occurs during a join. Shuffling involves redistributing data across nodes, which can be slow and resource-intensive.*
      
   ```python
   from pyspark.sql.functions import broadcast
   
   # Broadcast join with agents
   
   interactions_enriched_df = interactions_df_cleaned.join(
       broadcast(agents_df_cleaned), on='agent_id', how='left'
   )
   
   # Broadcast join with supervisors based on team
   interactions_enriched_df = interactions_enriched_df.join(
       broadcast(supervisors_df_cleaned),
       interactions_enriched_df.team == supervisors_df_cleaned.team,
       how='left'
   ).select(
       "interaction_id", "agent_id", "customer_id", "start_time", "end_time",
       "issue_type", "resolution", "agents_df_cleaned.name", "team",
       "supervisor_id", "supervisors_df_cleaned.name", "supervisors_df_cleaned.hire_date"
   ).withColumnRenamed("agents_df_cleaned.name", "agent_name") \
    .withColumnRenamed("supervisors_df_cleaned.name", "supervisor_name")

## Evaluation Metrics 
1. **Agent Dashboard**
    1. Latency: Time taken for data to reflect in the dashboard (seconds)
    2. Data Freshness: Time lag between data ingestion and dashboard availability (seconds)
    3. Throughput: Records processed per second (RPS)
    
2. **Supervisor Dashboard**
    1. Data Integrity: Consistency and completeness of each micro-batch (percentage).
    2. Batch Processing Time: Time to process a micro-batch (seconds).
    3. Resource Utilization: CPU and memory usage (percentage or MB).

# main.py

import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils import read_csv_with_schema, write_parquet_with_mode, read_parquet, write_parquet_with_compression, write_partitioned_parquet
from schema import interactions_schema, agents_schema, supervisors_schema
from constant import *
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("ContactCenterAnalytics").getOrCreate()

def main():
    """
    This function performs data ingestion, cleaning, transformation, enrichment, reporting, 
    analytics, optimization, and sets up Spark streaming for near real-time updates in dashboard 
    refreshing every 10 sec. 
    It reads CSV files with specified schemas, writes Parquet files, deduplicates data, fills 
    missing values, enriches data by joining DataFrames, calculates interaction duration, 
    resolution status, creates dashboard aggregates, caches data, calculates team performance, 
    generates detailed reports, writes partitioned Parquet files, uses Snappy compression for 
    storage efficiency, and defines a Spark streaming query for streaming updates.
    It includes a function get_dashboard_data to retrieve the latest dashboard data and 
    simulates near real-time updates every 10 seconds.
    """
    # Data Ingestion
    interactions_df = read_csv_with_schema(spark, interactions_path, interactions_schema)
    agents_df = read_csv_with_schema(spark, agents_path, agents_schema)
    supervisors_df = read_csv_with_schema(spark, supervisors_path, supervisors_schema)

    if interactions_df:
        write_parquet_with_mode(interactions_df, interactions_parquet_path)
    if agents_df:
        write_parquet_with_mode(agents_df, agents_parquet_path)
    if supervisors_df:
        write_parquet_with_mode(supervisors_df, supervisors_parquet_path)

    # Read from Parquet files
    interactions_df_parquet = read_parquet(spark, interactions_parquet_path)
    agents_df_parquet = read_parquet(spark, agents_parquet_path)
    supervisors_df_parquet = read_parquet(spark, supervisors_parquet_path)

    # Data Cleaning and Transformation
    interactions_df_dedup = interactions_df_parquet.dropDuplicates(['interaction_id'])
    agents_df_dedup = agents_df_parquet.dropDuplicates(['agent_id'])
    supervisors_df_dedup = supervisors_df_parquet.dropDuplicates(['supervisor_id'])

    interactions_df_cleaned = interactions_df_dedup.na.drop(subset=['interaction_id', 'agent_id'])
    agents_df_cleaned = agents_df_dedup.na.fill({"name": "Unknown"})
    supervisors_df_cleaned = supervisors_df_dedup.na.fill({"name": "Unknown"})

    # Data Enrichment
    interactions_enriched_df = interactions_df_cleaned.join(F.broadcast(agents_df_cleaned), on='agent_id', how='left')
    interactions_enriched_df = interactions_enriched_df.join(supervisors_df_cleaned, 
                                                            interactions_enriched_df.team == supervisors_df_cleaned.team,
                                                            how='left') \
                                                    .select("interaction_id", "agent_id", "customer_id", "start_time", "end_time", 
                                                            "issue_type", "resolution", "name", "team", 
                                                            "supervisor_id", "supervisors_df_cleaned.name", "supervisors_df_cleaned.hire_date") \
                                                    .withColumnRenamed("name", "agent_name") \
                                                    .withColumnRenamed("supervisors_df_cleaned.name", "supervisor_name")

    interactions_enriched_df = interactions_enriched_df.withColumn("interaction_duration",
        (F.unix_timestamp("end_time") - F.unix_timestamp("start_time")) / 60)  # duration in minutes

    interactions_enriched_df = interactions_enriched_df.withColumn("resolution_status",
        F.when(interactions_enriched_df.resolution == 'Resolved', 'Resolved').otherwise('Not Resolved'))

    # Reporting and Analytics
    dashboard_aggregates_df = interactions_enriched_df.groupBy("agent_id").agg(
        F.count("interaction_id").alias("num_interactions"),
        F.avg("interaction_duration").alias("avg_interaction_duration"),
        sum(F.when(interactions_enriched_df.resolution_status == 'Resolved', 1).otherwise(0)).alias("resolved_interactions"),
        F.count("interaction_id").alias("total_interactions")
    )

    dashboard_aggregates_df = dashboard_aggregates_df.withColumn(
        "resolution_rate", 
        dashboard_aggregates_df.resolved_interactions / dashboard_aggregates_df.total_interactions
    )

    dashboard_aggregates_df.cache()

    team_performance_df = interactions_enriched_df.groupBy("team").agg(
        F.count("interaction_id").alias("num_interactions"),
        F.avg("interaction_duration").alias("avg_interaction_duration"),
        sum(F.when(F.col("resolution_status") == 'Resolved', 1).otherwise(0)).alias("resolved_interactions"),
        F.count("interaction_id").alias("total_interactions")
    )

    team_performance_df = team_performance_df.withColumn(
        "resolution_rate", 
        team_performance_df.resolved_interactions / team_performance_df.total_interactions
    )

    detailed_reports_df = team_performance_df.join(F.broadcast(supervisors_df_cleaned), on='team', how='left')

    write_partitioned_parquet(detailed_reports_df, detailed_reports_df_parquet_path, "team")

    # Optimization
    # Use Snappy compression for storage and read efficiency
    if interactions_enriched_df:
        write_parquet_with_compression(interactions_enriched_df, interactions_enriched_path)
    if agents_df_cleaned:
        write_parquet_with_compression(agents_df_cleaned, agents_cleaned_path)
    if supervisors_df_cleaned:
        write_parquet_with_compression(supervisors_df_cleaned, supervisors_cleaned_path)


    # Data partition for efficient read by team and agent_id
    if interactions_enriched_df:
        write_partitioned_parquet(interactions_enriched_df, interactions_partitioned_path, "team")
    if agents_df_cleaned:
        write_partitioned_parquet(agents_df_cleaned, agents_partitioned_path, "team")
    if supervisors_df_cleaned:
        write_partitioned_parquet(supervisors_df_cleaned, supervisors_partitioned_path, "team")

    # Spark streaming for near real-time updates in dashboard refreshing every 10 sec

    # Define the query for streaming updates
    query = dashboard_aggregates_df.writeStream \
        .format("memory") \
        .queryName("agent_dashboard") \
        .outputMode("complete") \
        .start()

    # Function to get the latest dashboard data
    def get_dashboard_data():
        """
        Retrieves the latest dashboard data from the agent_dashboard memory table.

        Returns:
            DataFrame: The latest dashboard data.
        """
        # Retrieve data from the agent_dashboard memory table
        return spark.sql("""
            SELECT *
            FROM agent_dashboard
        """)

    # Simulate near real-time updates (every 10 seconds)
    import time

    while True:
        dashboard_data = get_dashboard_data()
        dashboard_data.show()
        time.sleep(10)
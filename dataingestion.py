# main.py

import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import pyspark.sql.functions as F
from utils import read_csv_with_schema, write_parquet_with_mode, read_parquet, write_parquet_with_compression, write_partitioned_parquet

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder.appName("ContactCenterAnalytics").getOrCreate()

# Define paths for CSV files
base_path = "abfss://<adfs_path>contact-center-analytics/"
interactions_path = base_path + "interactions.csv"
agents_path = base_path + "agents.csv"
supervisors_path = base_path + "supervisors.csv"

# Define paths for Parquet files
interactions_parquet_path = base_path + "interactions.parquet"
agents_parquet_path = base_path + "agents.parquet"
supervisors_parquet_path = base_path + "supervisors.parquet"

# Define paths for optimized Parquet files
optimized_base_path = base_path + "optimized_data/"
interactions_enriched_path = optimized_base_path + "interactions_enriched.parquet"
agents_cleaned_path = optimized_base_path + "agents_cleaned.parquet"
supervisors_cleaned_path = optimized_base_path + "supervisors_cleaned.parquet"

# Define paths for partitioned Parquet files
interactions_partitioned_path = optimized_base_path + "interactions_enriched_partitioned.parquet"
agents_partitioned_path = optimized_base_path + "agents_cleaned_partitioned.parquet"
supervisors_partitioned_path = optimized_base_path + "supervisors_cleaned_partitioned.parquet"

detailed_reports_df_parquet_path = optimized_base_path + "detailed_reports_df.parquet"

# Define schemas for the files
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
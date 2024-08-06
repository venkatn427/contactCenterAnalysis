# Import necessary libraries and initialize Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import unix_timestamp, when, count, avg, sum, col, broadcast

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
# Read data from CSV files and create DataFrames
try:
    interactions_df = spark.read.csv(interactions_path, schema=interactions_schema)
except Exception as e:
    print(f"Error reading interactions CSV file: {e}")

try:
    agents_df = spark.read.csv(agents_path, schema=agents_schema)
except Exception as e:
    print(f"Error reading agents CSV file: {e}")

try:
    supervisors_df = spark.read.csv(supervisors_path, schema=supervisors_schema)
except Exception as e:
    print(f"Error reading supervisors CSV file: {e}")

# Save DataFrames as Parquet for faster subsequent reads
try:
    interactions_df.write.parquet(interactions_parquet_path, mode='overwrite')
    agents_df.write.parquet(agents_parquet_path, mode='overwrite')
    supervisors_df.write.parquet(supervisors_parquet_path, mode='overwrite')
except Exception as e:
    print(f"Error writing Parquet files: {e}")

# Read from Parquet files and create DataFrames
try:
    interactions_df_parquet = spark.read.parquet(interactions_parquet_path)
    agents_df_parquet = spark.read.parquet(agents_parquet_path)
    supervisors_df_parquet = spark.read.parquet(supervisors_parquet_path)
except Exception as e:
    print(f"Error reading Parquet files: {e}")

# Data Cleaning and Transformation 
# Remove duplicates 
interactions_df_dedup = interactions_df.dropDuplicates(['interaction_id'])
agents_df_dedup = agents_df.dropDuplicates(['agent_id'])
supervisors_df_dedup = supervisors_df.dropDuplicates(['supervisor_id'])

# Handle missing values 
interactions_df_cleaned = interactions_df_dedup.na.drop(subset=['interaction_id', 'agent_id'])
agents_df_cleaned = agents_df_dedup.na.fill({"name": "Unknown"})
supervisors_df_cleaned = supervisors_df_dedup.na.fill({"name": "Unknown"})

# Data Enrichment
# Join interactions with agents based on the agentid using a left join
interactions_enriched_df = interactions_df_cleaned.join(broadcast(agents_df_cleaned), on='agent_id', how='left')

# Join with supervisors based on the team using a left join
# Left joins are efficient when we want to keep all records from the left table and match the records from the right table
interactions_enriched_df = interactions_enriched_df.join(supervisors_df_cleaned, 
                                                         interactions_enriched_df.team == supervisors_df_cleaned.team,
                                                         how='left') \
                                                   .select("interaction_id", "agent_id", "customer_id", "start_time", "end_time", 
                                                           "issue_type", "resolution", "name", "team", 
                                                           "supervisor_id", "supervisors_df_cleaned.name", "supervisors_df_cleaned.hire_date") \
                                                   .withColumnRenamed("name", "agent_name") \
                                                   .withColumnRenamed("supervisors_df_cleaned.name", "supervisor_name")

# Add new columns
# Calculate interaction duration
interactions_enriched_df = interactions_enriched_df.withColumn("interaction_duration",
    (unix_timestamp("end_time") - unix_timestamp("start_time")) / 60)  # duration in minutes

# Determine interaction resolution status
interactions_enriched_df = interactions_enriched_df.withColumn("resolution_status",
    when(interactions_enriched_df.resolution == 'Resolved', 'Resolved').otherwise('Not Resolved'))

# Reporting and Analytics
# Agent Dashboard - Insights by Agent (Very Low Latency)
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

# Supervisor Dashboard - Insights by Team (Focus on High Data Accuracy)
# Compute team performance metrics
team_performance_df = interactions_enriched_df.groupBy("team").agg(
    count("interaction_id").alias("num_interactions"),
    avg("interaction_duration").alias("avg_interaction_duration"),
    sum(when(col("resolution_status") == 'Resolved', 1).otherwise(0)).alias("resolved_interactions"),
    count("interaction_id").alias("total_interactions")
)

# Compute resolution rate
team_performance_df = team_performance_df.withColumn(
    "resolution_rate", 
    team_performance_df.resolved_interactions / team_performance_df.total_interactions
)

# Join with supervisor information for detailed reporting using broadcast join
detailed_reports_df = team_performance_df.join(broadcast(supervisors_df_cleaned), on='team', how='left')

# Write detailed reports as Parquet files (optimized for read efficiency) for faster subsequent reads
detailed_reports_df.write.parquet(detailed_reports_df_parquet_path, mode='overwrite',compression='snappy')

# Optimization
# Use Snappy compression for storage and read efficiency
try:
    interactions_enriched_df.write.parquet(interactions_enriched_path, mode='overwrite', compression='snappy')
    agents_df_cleaned.write.parquet(agents_cleaned_path, mode='overwrite', compression='snappy')
    supervisors_df_cleaned.write.parquet(supervisors_cleaned_path, mode='overwrite', compression='snappy')
except Exception as e:
    print(f"Error writing optimized Parquet files: {e}")

# Data partition for efficient read by team and agent_id
try:
    interactions_enriched_df.write.partitionBy("agent_id").parquet(interactions_partitioned_path, mode='overwrite', compression='snappy')
    agents_df_cleaned.write.partitionBy("team").parquet(agents_partitioned_path, mode='overwrite', compression='snappy')
    supervisors_df_cleaned.write.partitionBy("team").parquet(supervisors_partitioned_path, mode='overwrite', compression='snappy')
except Exception as e:
    print(f"Error writing parrtitioned Parquet files: {e}")
    
# Spark streaming for near real-time updates in dashboard refreshing every 10 sec
from pyspark.sql.streaming import StreamingQuery

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
from pyspark.sql import SparkSession
from constant import *

# Spark streaming for near real-time updates in dashboard refreshing every 10 sec

# Define the query for streaming updates
def main(spark: SparkSession, interactions_partitioned_path):
    df = spark.readStream.format("delta").load(interactions_partitioned_path)

    query = df.writeStream \
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
        
def team_performance(spark, detailed_reports_df_delta_path):
    df = spark.read.format("delta").load(detailed_reports_df_delta_path)
    # Reporting and Analytics using SQL
    team_performance_query = """
    SELECT
        team,
        supervisor_name,
        COUNT(interaction_id) AS num_interactions,
        AVG(interaction_duration) AS avg_interaction_duration,
        SUM(CASE WHEN resolution_status = 'Resolved' THEN 1 ELSE 0 END) AS resolved_interactions,
        COUNT(interaction_id) AS total_interactions,
        (SUM(CASE WHEN resolution_status = 'Resolved' THEN 1 ELSE 0 END) / COUNT(interaction_id)) AS resolution_rate
    FROM
        interactions_enriched
    JOIN 
        supervisor_df_cleaned 
    ON 
        interactions_enriched.supervisor_id = supervisor_df_cleaned.supervisor_id
    GROUP BY
        team, supervisor_name
    ORDER BY
        resolution_rate DESC
    """

    team_performance_df = spark.sql(team_performance_query)
    team_performance_df.show()
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("DashboardUpdates").getOrCreate()
    main(spark, interactions_partitioned_path)
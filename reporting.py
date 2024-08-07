from pyspark.sql import SparkSession
from constant import *

# Spark streaming for near real-time updates in dashboard refreshing every 10 sec

# Define the query for streaming updates
def main(spark: SparkSession, interactions_partitioned_path):
    """
    Streaming near real-time updates for agent dashboard.

    Args:
        spark (SparkSession): Spark session object.
        interactions_partitioned_path (str): Path to the partitioned interactions data.

    Returns:
        None
    """

    # Read the interactions data from the partitioned path
    df = spark.readStream.format("delta").load(interactions_partitioned_path)

    # Write the streaming data to memory table named 'agent_dashboard'
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

    # Infinite loop to simulate real-time updates
    while True:
        # Get the latest dashboard data
        dashboard_data = get_dashboard_data()
        # Show the dashboard data
        dashboard_data.show()
        # Sleep for 10 seconds before the next iteration
        time.sleep(10)
        
def team_performance(spark):
    """
    Retrieves team performance metrics from the detailed reports dataframe.

    Args:
        spark (SparkSession): The SparkSession object.
        detailed_reports_df_delta_path (str): The path to the detailed reports dataframe.

    Returns:
        None
    """

    # Read the detailed reports dataframe from the delta path
    df = spark.read.format("delta").load(interactions_partitioned_path)
    s_df = spark.read.format("delta").load(supervisors_partitioned_path)
    
    df.createOrReplaceTempView("interactions")
    s_df.createOrReplaceTempView("supervisor_df_cleaned")

    # Define the SQL query to retrieve team performance metrics
    team_performance_query = """
    SELECT
        team,  # Team name
        supervisor_name,  # Supervisor name
        COUNT(interaction_id) AS num_interactions,  # Number of interactions
        AVG(interaction_duration) AS avg_interaction_duration,  # Average interaction duration
        SUM(CASE WHEN resolution_status = 'Resolved' THEN 1 ELSE 0 END) AS resolved_interactions,  # Number of resolved interactions
        COUNT(interaction_id) AS total_interactions,  # Total number of interactions
        (SUM(CASE WHEN resolution_status = 'Resolved' THEN 1 ELSE 0 END) / COUNT(interaction_id)) AS resolution_rate  # Resolution rate
    FROM
        interactions_enriched
    JOIN 
        supervisor_df_cleaned 
    ON 
        interactions_enriched.supervisor_id = supervisor_df_cleaned.supervisor_id
    GROUP BY
        team, supervisor_name
    ORDER BY
        resolution_rate DESC  # Sort by resolution rate in descending order
    """

    # Execute the SQL query and retrieve the team performance dataframe
    team_performance_df = spark.sql(team_performance_query)

    # Display the team performance dataframe
    team_performance_df.show()
    
if __name__ == "__main__":
    spark = SparkSession.builder.appName("DashboardUpdates").getOrCreate()
    main(spark, interactions_partitioned_path)
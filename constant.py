# Define paths for CSV files
base_path = "abfss://<adfs_path>contact-center-analytics/"
interactions_path = base_path + "interactions.csv"
agents_path = base_path + "agents.csv"
supervisors_path = base_path + "supervisors.csv"

# Define paths for Parquet files
interactions_delta_path = base_path + "interactions.parquet"
agents_delta_path = base_path + "agents.parquet"
supervisors_delta_path = base_path + "supervisors.parquet"

# Define paths for optimized Parquet files
optimized_base_path = base_path + "optimized_data/"
interactions_enriched_path = optimized_base_path + "interactions_enriched.parquet"
agents_cleaned_path = optimized_base_path + "agents_cleaned.parquet"
supervisors_cleaned_path = optimized_base_path + "supervisors_cleaned.parquet"

# Define paths for partitioned Parquet files
interactions_partitioned_path = optimized_base_path + "interactions_enriched_partitioned.parquet"
agents_partitioned_path = optimized_base_path + "agents_cleaned_partitioned.parquet"
supervisors_partitioned_path = optimized_base_path + "supervisors_cleaned_partitioned.parquet"

detailed_reports_df_delta_path = optimized_base_path + "detailed_reports_df.parquet"
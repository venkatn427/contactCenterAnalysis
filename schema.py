from pyspark.sql.types import StructType, StructField, StringType, TimestampType

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
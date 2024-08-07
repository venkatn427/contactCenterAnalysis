from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast
import logging

# Setup logging
logger = logging.getLogger(__name__)

def read_csv_with_schema(spark: SparkSession, path: str, schema) -> DataFrame:
    """
    Read CSV file with a schema and return DataFrame.

    Args:
        spark (SparkSession): SparkSession object.
        path (str): Path to the CSV file.
        schema: Schema of the CSV file.

    Returns:
        DataFrame: DataFrame containing the data from the CSV file.
                  Returns None if an error occurs.
    """
    try:
        df = spark.read.csv(path, schema=schema)
        logger.info(f"Successfully read data from {path}")
        return df

    except Exception as e:
        logger.error(f"Error reading CSV file {path}: {e}")
        return None

def write_delta_with_mode(df: DataFrame, path: str) -> None:
    """
    Write DataFrame to Delta format with overwrite mode.

    Args:
        df (DataFrame): DataFrame to write to Delta.
        path (str): Path to write the Delta table.

    Returns:
        None

    Raises:
        Exception: If an error occurs while writing the Delta table.
    """
    try:
        df.write.format("delta").mode("overwrite").save(path)
        logger.info(f"Successfully wrote Delta table to {path}")
    except Exception as e:
        logger.error(f"Error writing Delta table {path}: {e}")

def read_delta(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Delta table from the specified path.

    Args:
        spark (SparkSession): Spark session object.
        path (str): Path to the Delta table.

    Returns:
        DataFrame: Delta table read from the specified path.

    Raises:
        Exception: If an error occurs while reading the Delta table.
    """
    try:
        df = spark.read.format("delta").load(path)
        logger.info(f"Successfully read Delta table from {path}")
        return df
    except Exception as e:
        logger.error(f"Error reading Delta table {path}: {e}")
        return None


def write_partitioned_delta(df: DataFrame, path: str, partition_cols: list) -> None:
    """
    Write partitioned DataFrame to Delta format.

    Args:
        df (DataFrame): DataFrame to write to Delta.
        path (str): Path to write the Delta table.
        partition_cols (list): List of columns to partition the data by.

    Returns:
        None

    Raises:
        Exception: If an error occurs while writing the Delta table.
    """
    try:
        df.write.format("delta").partitionBy(*partition_cols).mode("overwrite").save(path)
        logger.info(f"Successfully wrote partitioned Delta table to {path}")
    except Exception as e:
        logger.error(f"Error writing partitioned Delta table {path}: {e}")

def write_delta_as_table(df: DataFrame, table_name: str) -> None:
    """
    Write DataFrame to a Delta table with overwrite mode.

    Args:
        df (DataFrame): DataFrame to write to Delta.
        table_name (str): Name of the Delta table to create.

    Returns:
        None

    Raises:
        Exception: If an error occurs while writing the Delta table.
    """
    try:
        # Write DataFrame to Delta table with overwrite mode
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        logger.info(f"Successfully wrote Delta table as {table_name}")
    except Exception as e:
        logger.error(f"Error writing Delta table {table_name}: {e}")
        raise  # Re-raise the exception after logging it
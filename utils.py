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

def write_parquet_with_mode(df: DataFrame, path: str) -> None:
    """
    Write DataFrame to Parquet with overwrite mode.

    Args:
        df (DataFrame): DataFrame to write to Parquet.
        path (str): Path to write the Parquet file.

    Returns:
        None

    Raises:
        Exception: If an error occurs while writing the Parquet file.
    """
    try:
        df.write.parquet(path, mode='overwrite')
        logger.info(f"Successfully wrote Parquet file to {path}")
    except Exception as e:
        logger.error(f"Error writing Parquet file {path}: {e}")

def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Read Parquet file and return DataFrame.

    Args:
        spark (SparkSession): SparkSession object.
        path (str): Path to the Parquet file.

    Returns:
        DataFrame: DataFrame containing the data from the Parquet file.
                  Returns None if an error occurs.
    """
    try:
        df = spark.read.parquet(path)
        logger.info(f"Successfully read Parquet file from {path}")
        return df

    except Exception as e:
        logger.error(f"Error reading Parquet file {path}: {e}")
        return None



def write_parquet_with_compression(df: DataFrame, path: str) -> None:
    """
    Write DataFrame to Parquet with Snappy compression.

    Args:
        df (DataFrame): DataFrame to write to Parquet.
        path (str): Path to write the Parquet file.

    Raises:
        Exception: If an error occurs while writing the Parquet file.

    Returns:
        None
    """
    try:
        df.write.parquet(path, mode='overwrite', compression='snappy')
        logger.info(f"Successfully wrote Parquet file with Snappy compression to {path}")
    except Exception as e:
        logger.error(f"Error writing Parquet file with Snappy compression to {path}: {e}")



def write_partitioned_parquet(df: DataFrame, path: str, partition_col: str) -> None:
    """
    Write DataFrame to partitioned Parquet files.

    Args:
        df (DataFrame): DataFrame to write to Parquet.
        path (str): Path to write the Parquet file.
        partition_col (str): Name of the column to partition the data by.

    Raises:
        Exception: If an error occurs while writing the partitioned Parquet file.

    Returns:
        None
    """
    try:
        df.write.partitionBy(partition_col).parquet(path, mode='overwrite', compression='snappy')
        logger.info(f"Successfully wrote partitioned Parquet file to {path}")
    except Exception as e:
        logger.error(f"Error writing partitioned Parquet file {path}: {e}")

"""Load raw Olist CSV files into bronze Delta tables."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    """Read a raw CSV file, keeping every column as a string."""
    return (
        spark.read
        .option("header", True)
        .option("multiLine", True)  # review comments contain line breaks
        .option("escape", '"')      # quotes inside text are written as ""
        .csv(path)
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add lineage columns: when the row was loaded and from which file."""
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def write_table(df: DataFrame, table: str) -> None:
    """Replace the table with the given DataFrame."""
    df.write.mode("overwrite").saveAsTable(table)


def load_to_bronze(spark: SparkSession, source_path: str, table: str) -> int:
    """Read one CSV, add metadata, write it as a bronze table. Return the row count."""
    df = add_ingestion_metadata(read_csv(spark, source_path))
    write_table(df, table)
    return spark.table(table).count()
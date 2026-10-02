import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TAX_RATE = 1.20


def clean_data(df: DataFrame) -> DataFrame:
    """Remove invalid rows and add amount_with_tax."""
    return (
        df.filter(F.col("amount") > 0)
        .filter(F.col("name").isNotNull())
        .withColumn("amount_with_tax", F.col("amount") * TAX_RATE)
    )


if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("dataCleaning")
        .getOrCreate()
    )
    path = os.getenv("DATA_PATH", "orders.csv")
    raw = spark.read.csv(path, header=True, inferSchema=True)
    clean_data(raw).show()
    spark.stop()

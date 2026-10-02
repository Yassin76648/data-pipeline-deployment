import pytest
from pyspark.sql import SparkSession
from pyspark_job import clean_data

SCHEMA = "name string, amount double"


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_valid_records_are_kept(spark):
    df = spark.createDataFrame([("Alice", 100.0), ("Bob", 50.0)], SCHEMA)
    assert clean_data(df).count() == 2


def test_amount_less_or_equal_zero_removed(spark):
    df = spark.createDataFrame(
        [("Alice", 100.0), ("Bob", 0.0), ("Carl", -5.0)], SCHEMA
    )
    result = clean_data(df).collect()
    assert [r["name"] for r in result] == ["Alice"]


def test_null_names_removed(spark):
    df = spark.createDataFrame([("Alice", 100.0), (None, 50.0)], SCHEMA)
    result = clean_data(df).collect()
    assert len(result) == 1
    assert result[0]["name"] == "Alice"


def test_amount_with_tax_calculated_correctly(spark):
    df = spark.createDataFrame([("Alice", 100.0)], SCHEMA)
    row = clean_data(df).collect()[0]
    assert row["amount_with_tax"] == pytest.approx(120.0)
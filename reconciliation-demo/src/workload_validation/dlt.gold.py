# Delta Live Tables Gold Layer Implementation
# This module implements the gold layer logic for the DLT pipeline

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import *


@dlt.table(
    name="gold_reconciliation_summary",
    comment="Gold layer table containing reconciliation summary data"
)
def create_gold_reconciliation_summary():
    """
    Create gold layer reconciliation summary table
    
    Returns:
        DataFrame: Processed gold layer data
    """
    # Read from silver layer tables
    return (
        dlt.read("silver_source_data")
        .groupBy("business_date", "account_id")
        .agg(
            sum("amount").alias("total_amount"),
            count("*").alias("transaction_count"),
            max("last_updated").alias("last_processed")
        )
        .withColumn("processing_timestamp", current_timestamp())
    )


@dlt.table(
    name="gold_data_quality_metrics",
    comment="Gold layer table for data quality metrics"
)
def create_gold_data_quality_metrics():
    """
    Create gold layer data quality metrics table
    
    Returns:
        DataFrame: Data quality metrics
    """
    return (
        dlt.read("silver_source_data")
        .select(
            lit("reconciliation_demo").alias("pipeline_name"),
            count("*").alias("total_records"),
            sum(when(col("amount").isNull(), 1).otherwise(0)).alias("null_amounts"),
            current_timestamp().alias("metric_timestamp")
        )
    )


@dlt.table(
    name="gold_aggregated_positions",
    comment="Gold layer aggregated positions by account"
)
def create_gold_aggregated_positions():
    """
    Create gold layer aggregated positions
    
    Returns:
        DataFrame: Aggregated position data
    """
    return (
        dlt.read("silver_source_data")
        .filter(col("status") == "active")
        .groupBy("account_id", "position_type")
        .agg(
            sum("amount").alias("total_position"),
            avg("amount").alias("average_position"),
            count("*").alias("position_count")
        )
        .withColumn("snapshot_date", current_date())
    )
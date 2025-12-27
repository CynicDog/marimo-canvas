import marimo

__generated_with = "0.12.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import os
    RAW_BASE_PATH = os.getenv("RAW_BASE_PATH", "/data/raw")
    return RAW_BASE_PATH, os


@app.cell
def _():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("mta-marimo")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    return SparkSession, spark


@app.cell
def _(RAW_BASE_PATH, spark):
    raw_df = (     
        spark.read     
        .option("recursiveFileLookup", "true")     
        .json(RAW_BASE_PATH) 
    ) 
    raw_df.show(truncate=False)

    BRONZE_PATH = "/delta/mta_bronze"
    raw_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("/delta/mta_bronze")
    return BRONZE_PATH, raw_df


@app.cell
def _(BRONZE_PATH, spark):
    # Read Delta table
    bronze_df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    # Convert to pandas (limit first for safety)
    bronze_pdf = bronze_df.limit(1000).toPandas()

    # Display in Marimo
    bronze_pdf
    return bronze_df, bronze_pdf


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.12.8"
app = marimo.App(
    width="medium",
    app_title="Declarative Data Processing",
    layout_file="layouts/declarative-data-processing.grid.json",
    auto_download=["html"],
)


@app.cell
def _():
    import os

    RAW_BASE_PATH = os.getenv("RAW_BASE_PATH", "/data/raw")
    BRONZE_PATH = "/delta/mta_bronze"
    SILVER_PATH = "/delta/mta_silver"
    return BRONZE_PATH, RAW_BASE_PATH, SILVER_PATH, os


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
def _():
    from functools import wraps
    from pyspark.sql import functions as F

    def table(name, partition_cols=None, path=None, mode="overwrite", merge_schema=True):
        """
        Local @table decorator for writing a DataFrame to Delta.

        This decorator wraps a function that returns a Spark DataFrame, and automatically
        writes the DataFrame to a Delta table at the specified path. It also supports 
        partitioning and optional schema evolution.

        Parameters:
        - name: str
            Logical name of the table. Used for default path if `path` is not provided.
        - partition_cols: Optional[list[str]]
            List of columns to partition the Delta table by. Default is None (no partitioning).
        - path: Optional[str]
            Path where the Delta table will be saved. Defaults to "./{name}" if not provided.
        - mode: str
            Write mode for the table. Typical values: "overwrite", "append". Default is "overwrite".
        - merge_schema: bool
            Whether to enable Delta schema evolution (merge new columns into existing table). Default is True.

        Returns:
            Decorator that writes the returned DataFrame to Delta and returns the original DataFrame.

        Usage:
            # Example 1: Basic usage
            @table(name="mta_bronze", path="./delta/mta_bronze")
            def bronze():
                df = spark.read.option("recursiveFileLookup", "true").json(RAW_BASE_PATH)
                return df

            bronze_df = bronze()  # writes Delta table and returns DataFrame

            # Example 2: With partitioning and custom mode
            @table(name="orders_silver", path="./delta/orders_silver", partition_cols=["year", "month"], mode="append")
            def orders_silver():
                df = spark.read.table("bronze_view").filter("topic = 'orders'")
                return df

            orders_silver_df = orders_silver()

            # Example 3: Disable schema merge if not needed
            @table(name="mta_bronze_no_merge", path="./delta/mta_bronze", merge_schema=False)
            def bronze_no_merge():
                df = spark.read.json(RAW_BASE_PATH)
                return df
        """
        if partition_cols is None:
            partition_cols = []

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                df = func(*args, **kwargs)
                write_path = path or f"./{name}"
                writer = df.write.format("delta").mode(mode)
                if partition_cols:
                    writer = writer.partitionBy(*partition_cols)
                if merge_schema:
                    writer = writer.option("mergeSchema", "true")
                writer.save(write_path)
                print(f"Table '{name}' written to {write_path} (merge_schema={merge_schema})")
                return df
            return wrapper
        return decorator


    def temporary_view(name=None):
        """
        Local @temporary_view decorator for creating a Spark temporary view from a DataFrame.

        This decorator wraps a function that returns a Spark DataFrame, and automatically 
        registers the DataFrame as a temporary view in Spark SQL. This allows you to query 
        the DataFrame using SQL later in the session.

        Parameters:
        - name: Optional[str]
            The name of the temporary view to create. 
            If not provided, defaults to the name of the decorated function.

        Usage:
            @temporary_view(name="bronze_view")
            def bronze_view():
                return bronze_df

            # The view 'bronze_view' can now be queried via spark.sql("SELECT * FROM bronze_view")
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                df = func(*args, **kwargs)
                view_name = name or func.__name__
                df.createOrReplaceTempView(view_name)
                print(f"Temporary view '{view_name}' created")
                return df
            return wrapper
        return decorator


    def expect_or_drop(expectation_name, condition):
        """
        Decorator to drop rows from a DataFrame that do not meet the specified condition.

        Parameters:
        - expectation_name: str
            A descriptive name for the expectation (used in logs).
        - condition: str
            A SQL expression used to filter the DataFrame. Rows failing this condition are dropped.

        Returns:
            Decorator that applies the expectation to a DataFrame-returning function.

        Usage:
            @expect_or_drop("valid_quantity", "quantity > 0")
            def orders_silver():
                return spark.read.table("bronze_view")
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                df = func(*args, **kwargs)
                df = df.filter(condition)
                print(f"Expectation '{expectation_name}' applied: dropped failing rows")
                return df
            return wrapper
        return decorator


    def expect_all(rules: dict):
        """
        Decorator to mark rows failing any rule by adding an 'is_quarantined' column.

        Parameters:
        - rules: dict
            A mapping of rule names to SQL expressions. 
            Rows that fail any of these rules will have 'is_quarantined' = True.

        Returns:
            Decorator that adds the 'is_quarantined' column to a DataFrame-returning function.

        Usage:
            @expect_all({
                "valid_price": "price BETWEEN 0 AND 100",
                "valid_id": "book_id IS NOT NULL"
            })
            @temporary_view(name="books_raw")
            def books_raw():
                return spark.read.table("bronze_view")
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                df = func(*args, **kwargs)
                quarantine_expr = " OR ".join(f"NOT({v})" for v in rules.values())
                df = df.withColumn("is_quarantined", F.expr(quarantine_expr))
                print(f"Expectations {list(rules.keys())} applied: 'is_quarantined' column added")
                return df
            return wrapper
        return decorator


    def expect_or_fail(expectation_name, condition):
        """
        Decorator that fails the pipeline if any row does not meet the specified condition.

        Parameters:
        - expectation_name: str
            A descriptive name for the expectation (used in logs).
        - condition: str
            A SQL expression used to check each row. Pipeline fails if any row violates it.

        Returns:
            Decorator that enforces the expectation strictly.

        Usage:
            @expect_or_fail("positive_total", "total > 0")
            @table(name="critical_orders")
            def critical_orders():
                return spark.read.table("bronze_view")
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                df = func(*args, **kwargs)
                failing_count = df.filter(f"NOT({condition})").count()
                if failing_count > 0:
                    raise ValueError(f"Expectation '{expectation_name}' failed: {failing_count} rows do not meet the condition")
                print(f"Expectation '{expectation_name}' passed: all rows meet the condition")
                return df
            return wrapper
        return decorator

    return (
        F,
        expect_all,
        expect_or_drop,
        expect_or_fail,
        table,
        temporary_view,
        wraps,
    )


@app.cell
def _(BRONZE_PATH, F, RAW_BASE_PATH, spark, table):
    @table(
        name="mta_bronze",
        partition_cols=[],
        path=BRONZE_PATH,
        mode="overwrite"
    )
    def bronze():
        df = (
            spark.read
            .option("recursiveFileLookup", "true")
            .json(RAW_BASE_PATH)
        )
        df = df.withColumn("ingest_ts", F.current_timestamp())
        return df

    # Run the bronze pipeline
    bronze_df = bronze()
    return bronze, bronze_df


@app.cell
def _(BRONZE_PATH, spark, temporary_view):
    @temporary_view(name="bronze_view")
    def bronze_view():
        df = spark.read.format("delta").load(BRONZE_PATH)
        return df
    
    # Create the temp view
    tmp_v = bronze_view()

    _pdf = tmp_v.limit(1000).toPandas()
    _pdf
    return bronze_view, tmp_v


@app.cell
def _(BRONZE_PATH, F, expect_or_drop, spark, temporary_view):
    @temporary_view(name="silver_view")
    @expect_or_drop("assigned_trains_only", "is_assigned = True")
    def silver_view():
        """
        Reads bronze Delta table, drops rows with missing actual_track or non-assigned trains,
        and returns a cleaned DataFrame ready for silver layer.
        """
        df = spark.read.format("delta").load(BRONZE_PATH)
        df = df.withColumn("silver_ingest_ts", F.current_timestamp())
        return df


    _tmp_v = silver_view()  

    _silver_pdf = _tmp_v.limit(1000).toPandas()
    _silver_pdf
    return (silver_view,)


if __name__ == "__main__":
    app.run()

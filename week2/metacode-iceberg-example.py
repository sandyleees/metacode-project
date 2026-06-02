from pyspark.sql import SparkSession

WAREHOUSE = "s3://metacode-iceberg-example/warehouse/"

spark = (
    SparkSession.builder.appName("IcebergGlueLab")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
    .getOrCreate()
)

spark.sql("""
    CREATE TABLE glue_catalog.ad_lakehouse.event (
        event_id bigint,
        event_time timestamp,
        user_id string,
        amount double
    )
    PARTITIONED BY (day(event_time))
    LOCATION 's3://metacode-iceberg-example/event/'
    TBLPROPERTIES (
        'table_type' = 'ICEBERG',
        'format' = 'parquet',
        'format_version' = '2'
    )
""")

spark.sql("""
    INSERT INTO glue_catalog.ad_lakehouse.event VALUES
    (1, TIMESTAMP '2026-04-10 09:00:00', 'u1', 100.0),
    (2, TIMESTAMP '2026-04-10 10:00:00', 'u2', 200.0)
""")


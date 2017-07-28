from pyspark.sql import SparkSession
from pyspark import SparkContext

def basic_datasource_example(spark):
    # # $example on:generic_load_save_functions$
    # df = spark.read.load("examples/src/main/resources/users.parquet")
    # df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    # # $example off:generic_load_save_functions$
    #
    # # $example on:write_partitioning$
    # df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    # # $example off:write_partitioning$
    #
    # # $example on:write_partition_and_bucket$
    # df = spark.read.parquet("examples/src/main/resources/users.parquet")
    # (df
    #  .write
    #  .partitionBy("favorite_color")
    #  .bucketBy(42, "name")
    #  .saveAsTable("people_partitioned_bucketed"))
    # # $example off:write_partition_and_bucket$

    # $example on:manual_load_options$
    business_DF = spark.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_business.parquet")
    business_DF.createOrReplaceTempView("business")
    checkin_DF = spark.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_checkin.parquet")
    checkin_DF.createOrReplaceTempView("checkin")
    review_DF = spark.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_review.parquet")
    review_DF.createOrReplaceTempView("review")
    tip_DF = spark.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_tip.parquet")
    tip_DF.createOrReplaceTempView("tip")
    user_DF = spark.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_user.parquet")
    user_DF.createOrReplaceTempView("user")
    teenagers = spark.sql("SELECT business_id FROM business where id = '0DI8Dt2PJp07XkVvIElIcQ'")
    teenagers.show()
    # df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
    # # $example off:manual_load_options$
    #
    # # $example on:write_sorting_and_bucketing$
    # df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    # # $example off:write_sorting_and_bucketing$
    #
    # # $example on:direct_sql$
    # df = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
    # # $example off:direct_sql$
    #
    # spark.sql("DROP TABLE IF EXISTS people_bucketed")
    # spark.sql("DROP TABLE IF EXISTS people_partitioned_bucketed")


    if __name__ == "__main__":
        spark = SparkContext \
            .builder \
            .appName("Python Spark SQL data source example") \
            .getOrCreate()

    basic_datasource_example(spark)
    # parquet_schema_merging_example(spark)
    # json_dataset_example(spark)
    # jdbc_dataset_example(spark)

    spark.stop()
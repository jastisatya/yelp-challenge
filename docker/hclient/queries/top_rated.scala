val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val pbusiness = sqlContext.read.parquet("/output/yelp_academic_dataset_business.parquet")
pbusiness.registerTempTable("business")
val preview = sqlContext.read.parquet("/output/yelp_academic_dataset_review.parquet")
preview.registerTempTable("review")
val allrecords = sqlContext.sql("select b.name, count(*) as top_stars from business b join review r on b.business_id = r.business_id where r" +
  ".stars=5 group by b.name order by top_stars desc ").limit(50)
allrecords.show(50)
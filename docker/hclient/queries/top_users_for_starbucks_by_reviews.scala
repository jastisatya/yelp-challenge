val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val pbusiness = sqlContext.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_business.parquet")
pbusiness.registerTempTable("business")
val preview = sqlContext.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_review.parquet")
preview.registerTempTable("review")
val puser = sqlContext.read.parquet("/yelp-dataset-extracted/yelp_academic_dataset_user.parquet")
puser.registerTempTable("user")
val allrecords = sqlContext.sql("select u.name, count(*) as top_reviewers_starbucks from user u join review r on u.user_id = r.user_id " +
  " join business b on b.business_id = r.business_id where b.name='Starbucks' group by u.name order by top_reviewers_starbucks desc ").limit(20)
allrecords.show(20)
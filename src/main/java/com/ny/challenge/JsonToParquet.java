package com.ny.challenge;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class JsonToParquet {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new RuntimeException("expecting HDFS input path");
        }


        String allFiles = args[0];
        String[] files = allFiles.split(",");

        SparkConf conf = new SparkConf()
                .setAppName(JsonToParquet.class.getName());
        JavaSparkContext context = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);
        for (String file : files) {
            parseJsonToParquet(sqlContext, file, file.split("\\.")[0] + ".parquet");
        }
    }

    private static void parseJsonToParquet(SQLContext sqlContext, String inputFilename, String outputFilename) {
        Dataset<Row> domains = sqlContext.read().json(inputFilename);
        domains.write().parquet(outputFilename);
    }
}

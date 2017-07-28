package com.ny.challenge;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.Closeable;
import java.io.IOException;

public class JsonToParquet extends Configured implements Tool, Closeable {

    public static final String INPUT_FILES = "spark.input.path";
    public static final String OUTPUT_PATH = "spark.output.path";
    public static final String IS_RUN_LOCALLY = "spark.is.run.local";
    public static final String DEFAULT_FS = "spark.default.fs";
    public static final String NUM_PARTITIONS = "spark.num.partitions";

    private SQLContext sqlContext;

    private JavaSparkContext javaSparkContext;

    protected <T> JavaSparkContext getJavaSparkContext(final boolean isRunLocal,
                                                       final String defaultFs,
                                                       final Class<T> tClass) {
        final SparkConf sparkConf = new SparkConf()
                //Set spark conf here.
                // after one gets spark context you can set hadoop configuration for InputFormats
                .setAppName(tClass.getSimpleName())
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        if (isRunLocal) {
            sparkConf.setMaster("local[*]");
        }

        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        if (defaultFs != null) {
            sparkContext.hadoopConfiguration().set("fs.defaultFS", defaultFs);
        }

        return sparkContext;
    }

    public SQLContext getSqlContext() {
        return sqlContext;
    }

    public void close() throws IOException {
        IOUtils.closeQuietly(javaSparkContext);
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new JsonToParquet(), args);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        String inputFiles = conf.get(INPUT_FILES);
        String outputPath = conf.get(OUTPUT_PATH);
        String[] files = inputFiles.split(",");
//        String allFiles = args[0];
//        String[] files = allFiles.split(",");

        //Get spark context, This is the central context , which can be wrapped in Any Other context
        javaSparkContext = getJavaSparkContext(conf.getBoolean(IS_RUN_LOCALLY, Boolean.FALSE),
                                               conf.get(DEFAULT_FS), JsonToParquet.class);
        sqlContext = new SQLContext(javaSparkContext);

        for (String file : files) {
            Dataset<Row> domains = sqlContext.read().json(file);
            String[] split = file.split("/");
            String outputFileName = split[split.length-1].split("\\.")[0]+".parquet";
            domains.write().parquet(outputPath+"/"+outputFileName);
        }

        return 0;
    }
}
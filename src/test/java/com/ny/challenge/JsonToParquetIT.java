package com.ny.challenge;

/**
 * Created by levon on 28/07/17.
 */

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertTrue;

public class JsonToParquetIT {

    // private final Configuration conf = new Configuration();
    private static FileSystem fs;
    private static String baseDir;
    private static String inputFiles;
    private static String outputDir;
    private static String outFilename;
    private static final String NEW_LINE_DELIMETER = "\n";

    private static final String JSON = "{\"business_id\":\"a1Ba6XeIOP48e64YFD0dMw\",\"name\":\"La Prep\",\"neighborhood\":\"Ville-Marie\"," +
            "\"address\":\"2000 Mansfield Street, Suite 104\",\"city\":\"Montréal\",\"state\":\"QC\",\"postal_code\":\"H3A 2Z6\"," +
            "\"latitude\":45.5023462,\"longitude\":-73.5738073,\"stars\":4.0,\"review_count\":3,\"is_open\":1,\"attributes\":[\"Caters: True\"]," +
            "\"categories\":[\"Sandwiches\",\"Breakfast & Brunch\",\"Salad\",\"Restaurants\"],\"hours\":[\"Monday 6:30-17:0\",\"Tuesday " +
            "6:30-17:0\",\"Wednesday 6:30-17:0\",\"Thursday 6:30-17:0\",\"Friday 6:30-17:0\"],\"type\":\"business\"}";

    @BeforeClass
    public static void startup() throws Exception {

        Configuration conf = new Configuration();
        fs = FileSystem.getLocal(conf);
        baseDir = "/tmp/spark/JsonToParquet/" + UUID.randomUUID().toString();
        outFilename = UUID.randomUUID().toString() + ".parquet";
        inputFiles = baseDir + "/" + outFilename;
        outputDir = baseDir + "/output";

        File tempFile = new File(inputFiles);

        //Write the data into the local filesystem
        FileUtils.writeStringToFile(tempFile, JSON, "UTF-8");
        FileUtils.writeStringToFile(tempFile, NEW_LINE_DELIMETER, "UTF-8", true);
        //FileUtils.writeStringToFile(tempFile, content, "UTF-8", true);

    }

    @AfterClass
    public static void cleanup() throws Exception {
        //Delete the local filesystem folder after the Job is done
        fs.delete(new Path(baseDir), true);
    }

    @Test
    public void jsonToParquetTest() throws Exception {

        JsonToParquet jsonToParquet = new JsonToParquet();
        // Any argument passed with -DKey=Value will be parsed by ToolRunner
        String[] args = new String[]{"-D" + JsonToParquet.INPUT_FILES + "=" + inputFiles,
                "-D" + JsonToParquet.OUTPUT_PATH + "=" + outputDir,
                "-D" + JsonToParquet.IS_RUN_LOCALLY + "=true",
                "-D" + JsonToParquet.DEFAULT_FS + "=file:///",
                "-D" + JsonToParquet.NUM_PARTITIONS + "=1"};

        // calling the spark job
        ToolRunner.run(jsonToParquet, args);

        //Read the data from the outputfile
        final Dataset<Row> parquet = jsonToParquet.getSqlContext().read().parquet(outputDir + "/" + outFilename);
        parquet.collectAsList().stream().forEach(row -> {
            assertTrue(JSON.contains(row.getString(0)));
        });

    }

}
package com.example.spark.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.BeforeClass;
import org.junit.Test;

public class SparkStreamingTest {

    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static JavaStreamingContext sctx;


    @BeforeClass
    public static void init() {
        sparkConf = new SparkConf().setAppName("SparkStreaming").setMaster("local[*]");
        ctx = new JavaSparkContext(sparkConf);
        sctx = new JavaStreamingContext(ctx, Durations.seconds(5));

    }

    @Test
    public void sparkStreaming() throws InterruptedException {

        JavaReceiverInputDStream<String> lines = sctx.socketTextStream("localhost", 9999);
        lines.print();

        sctx.start();
        sctx.awaitTermination();
    }
}

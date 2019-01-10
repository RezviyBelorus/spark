package com.example.spark.rdd;

import org.apache.parquet.format.Statistics;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

public class JavaRDDTest {
    static SparkConf sparkConf;
    static JavaSparkContext sc;

    @BeforeClass
    public static void init() {
        sparkConf = new SparkConf()
                .setAppName("My app")
                .setMaster("local[*]");

        sc = new JavaSparkContext(sparkConf);
    }

    @Test
    public void stats() {
        JavaRDD<String> keys = sc.parallelize(Arrays.asList("A", "B", "C"));
        JavaRDD<Integer> ints = sc.parallelize(Arrays.asList(1, 2, 3));
        JavaPairRDD<String, Integer> zip = keys.zip(ints);

        zip.collect().forEach(System.out::println);

    }
}

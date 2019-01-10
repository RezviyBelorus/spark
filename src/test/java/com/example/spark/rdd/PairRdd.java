package com.example.spark.rdd;

import com.example.spark.model.Student;
import org.apache.spark.Accumulator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;


public class PairRdd {
    static SparkConf sparkConf;
    static JavaSparkContext sc;

    private JavaPairRDD<Integer, String> input = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"),
            new Tuple2<>(2, "B"),
            new Tuple2<>(2, "B")));

    private JavaPairRDD<Integer, String> input_2 = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"),
            new Tuple2<>(4, "B"),
            new Tuple2<>(5, "B")));

    private JavaPairRDD<Integer, String> input_3 = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"),
            new Tuple2<>(4, "C"),
            new Tuple2<>(5, "D")));

    @BeforeClass
    public static void init() {
        sparkConf = new SparkConf()
                .setAppName("My app")
                .setMaster("local[*]");

        sc = new JavaSparkContext(sparkConf);
    }

    @Test
    public void filterBySecond() {

        List<Tuple2<Integer, String>> filtered = input.filter(tupple2 -> tupple2._2.equals("A"))
                .collect();

        System.out.println(filtered);

        assertEquals(new Integer(1), filtered.get(0)._1);
        assertEquals("A", filtered.get(0)._2);

    }

    @Test
    public void mapValues() {
        JavaPairRDD<Integer, String> input = sc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"),
                new Tuple2<>(2, "B"),
                new Tuple2<>(2, "B")));

        input.setName("New RDD name");

        System.out.println("---ID---");
        System.out.println(input.id());

        JavaPairRDD<Integer, String> output = input.mapValues(s -> s);

        System.out.println("---Debug string---");
        System.out.println(input.toDebugString());
        assertEquals(input.collect(), output.collect());
    }

    @Test
    public void singlePartition() {
        IntStream range = IntStream.range(0, 1000_000);

        List<Integer> integers = range.boxed().collect(Collectors.toList());

        JavaRDD<Integer> parallelize = sc.parallelize(integers);

        parallelize.coalesce(3);

        long begin = System.currentTimeMillis();

        Integer reduce = parallelize.reduce((integer, integer2) -> integer + integer2);

        System.out.println("sum=" + reduce);

        long end = System.currentTimeMillis();

        System.out.println("ms=" + (end - begin));

        System.out.println("Partitions=" + parallelize.getNumPartitions());
    }

    @Test
    public void groupBy() {

        JavaPairRDD<Integer, Iterable<String>> byKey = input.groupByKey();

        System.out.println("groupByKey=" + byKey.collect());

        JavaPairRDD<String, Iterable<Tuple2<Integer, String>>> groupedBy = input.groupBy(tuple2 -> tuple2._2);
        System.out.println("groupBy=" + groupedBy.collect());

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<String>>> cogroup = input.cogroup(input_2);
        System.out.println("coGroup=" + cogroup.collect());

    }

    @Test
    public void join() {
        JavaPairRDD<Integer, Tuple2<String, String>> join = input.join(input_3);
        List<Tuple2<Integer, Tuple2<String, String>>> collect = join.collect();

        JavaPairRDD<Integer, Tuple2<Optional<String>, String>> rightOuterJoin = input.rightOuterJoin(input_3);
        List<Tuple2<Integer, Tuple2<Optional<String>, String>>> rightOuterCollect = rightOuterJoin.collect();

        List<Tuple2<Integer, Tuple2<String, Optional<String>>>> leftJoin = input.leftOuterJoin(input_3).collect();

        System.out.println("inner join=" + collect);
        System.out.println("Right outer join=" + rightOuterCollect);
        System.out.println("Left outer join=" + leftJoin);


    }

    @Test
    public void lookup() {
        //finds all values by key
        List<String> lookup = input.lookup(1);
        System.out.println("lookedUp=" + lookup);
    }

    @Test
    public void hashPartitioner() {
        JavaPairRDD<Integer, String> repartitioned = input.partitionBy(new HashPartitioner(100));

    }

    @Test
    public void textFiles() {
        JavaPairRDD<String, String> fileAsKeyAndDataAsValue = sc.wholeTextFiles("pathToFolder");

    }

    @Test
    public void objectFiles() {
        Student student = new Student("Alex", 22);

        JavaRDD<Student> studentsRDD = sc.parallelize(Arrays.asList(student));

        File file = new File("/home/alex/Downloads/student");
        if (!file.exists()) {
            studentsRDD.saveAsObjectFile("/home/alex/Downloads/student");
        }

        JavaRDD<Student> objectJavaRDD = sc.objectFile("/home/alex/Downloads/student");

        System.out.println(objectJavaRDD.map(Student::getName).collect());

    }

    @Test
    public void jdbc() {
//        new JdbcRDD(sc, );
    }

    @Test
    public void accunmulate() {
        Accumulator<Integer> accumulator = sc.accumulator(0);
        System.out.println("accum before=" + accumulator.value());

        List<Integer> collect = input.map(tuple -> {
            accumulator.add(1);
            return tuple._1();
        })
                .collect();

        System.out.println("accum after=" + accumulator.value());


    }

    @Test
    public void mapPartitions() {
//        input.mapPartitions();

    }

    @AfterClass
    public static void closeContext() {
        sc.close();
    }
}


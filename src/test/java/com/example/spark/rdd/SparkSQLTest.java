package com.example.spark.rdd;

import com.example.spark.model.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkSQLTest {
    static SparkConf sparkConf;
    static JavaSparkContext sc;
    static SQLContext sqlContext;

    private JavaRDD<Person> names = sc.parallelize(Arrays.asList(
            new Person("alex", "fom"),
            new Person("alex", "fom"),
            new Person("egor", "bugov")));

    @BeforeClass
    public static void init() {
        sparkConf = new SparkConf()
                .setAppName("My app")
                .setMaster("local[*]");

        sc = new JavaSparkContext(sparkConf);

        sqlContext = new SQLContext(sc);
    }

    @Test
    // TODO: 1/4/19 doesn't work
    public void sparkSQLIntroTest() {
        String pathToJson = "/mnt/0d766a07-62ea-4c27-9975-227da9264f7a/alex/IdeaProjects/spark/src/main/resources/test.json";
        String pathToCsv = "/mnt/0d766a07-62ea-4c27-9975-227da9264f7a/alex/IdeaProjects/spark/src/main/resources/csv.csv";

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("surname", DataTypes.StringType, true)
        });

        Dataset<Row> rowDataset = sqlContext.jsonFile(pathToJson, schema);
        rowDataset.registerTempTable("json");
        Dataset<Row> sql = sqlContext.sql("SELECT name, surname FROM json");
        sql.show();


//        rowDataset.registerTempTable("test");

//        Dataset<Row> csv = sqlContext.read().json(pathToJson);
//        csv.registerTempTable("json");
//        csv.printSchema();
//        csv.show();


//        Dataset<Row> select_name_from_test = sqlContext.sql("SELECT name, age FROM test");

//        select_name_from_test.foreach(row -> System.out.println(row));

    }

    @Test
    public void applySchema() {
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("surname", DataTypes.TimestampType, true)
        });

        Dataset<Row> rowDataset = sqlContext.applySchema(names, Person.class);
        rowDataset.registerTempTable("test");
        rowDataset.printSchema();

        Dataset<Row> select_name_from_test = sqlContext.sql("SELECT name, surname, COUNT(*) as count FROM test GROUP BY name, surname");
        select_name_from_test.show();
    }
}

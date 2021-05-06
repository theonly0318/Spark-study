package top.theonly.spark.jav.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 简化代码
 */
public class SparkWordCountSimplify {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("java-wordcount").setMaster("local");
        // java中创建的是JavaSparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 文件路径
        String path = "C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt";

        // 使用lambda表达式
//
//        JavaRDD<String> lines = context.textFile(path);
//
//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
//
//        JavaPairRDD<String, Integer> pairWords = words.mapToPair(s -> new Tuple2<>(s, 1));
//
//        JavaPairRDD<String, Integer> res = pairWords.reduceByKey((v1, v2) -> v1 + v2);
//
//        res.foreach(tuple2 -> System.out.println(tuple2));

        // 将上面的简化；链式编程
        context.textFile(path)
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((v1, v2) -> v1 + v2)
                .foreach(tuple2 -> System.out.println(tuple2));
        long end = System.currentTimeMillis();
        System.out.println("消耗时长："+(end-start)+"毫秒");
    }
}

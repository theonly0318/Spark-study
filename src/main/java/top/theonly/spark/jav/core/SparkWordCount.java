package top.theonly.spark.jav.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * SparkConf：Spark配置项
 *      1). setAppName(String name)，设置应用重程序的名称
 *      2). setMaster(String master) 设置运行模式
 *          - local：本地模式；在eclipse或IDEA中开发Spark，在本地运行
 *          - standalone：Spark自己提供的资源调度框架，支持分布式搭建
 *          - YARN：Hadoop生态圈中的资源调度框架，常用
 *          - mesos：Apache下的开源分布式资源管理框架，它被称为是分布式系统的内核。
 *      3). setJars(String[] jars)
 *      4). set(String key, String value)
 *          - spark.executor.memory   4g
 *              内存
 *          - spark.eventLog.enabled  true
 *              事件日志
 *          - spark.serializer        org.apache.spark.serializer.KryoSerializer
 *              序列化
 *
 * SparkContext（JavaSparkContext）：Spark上下文对象
 *      是Spark通往集群的唯一通道
 *
 * RDD（JavaRDD、JavaPairRDD）：
 *      Resilient Distributed Dateset，弹性分布式数据集。
 *   RDD的五大特性：
 *      1.RDD是由一系列的partition组成的。
 *      2.算子（函数，例如map，flatMap）是作用在每一个partition（split）上的。
 *      3.RDD之间有一系列的依赖关系。
 *      4.分区器是作用在K,V格式的RDD上（RDD中的数据是一个个的Tuple2，RDD[(String, Int)]）。
 *      5.RDD提供一系列最佳的计算位置，利于数据处理的本地化。（数据不移动，计算移动）
 *
 * RDD实际上不存储数据，partition也不存储数据（这里方便理解，暂时理解为存储数据）
 *
 * SparkContext.textFile() 底层读取HDFS文件调用的是Hadoop MR读取HDFS文件的方法
 *      首先对block切片（split），默认一个block一个切片，一个切片对应一个RDD的partition，切片的个数就是RDD partition的个数
 *
 *
 * Spark 三类算子
 *      1、Transformation 转换算子
 *          Transformations类算子是一类算子（函数）叫做转换算子
 *          如map,flatMap,reduceByKey等。
 *          Transformations算子是延迟执行，也叫懒加载执行。必须有触发算子触发才会执行
 *      2、Action 触发算子
 *          Action类算子也是一类算子（函数）叫做行动算子，如foreach,collect，count等
 *          触发Transformations类算子执行
 *          Transformations类算子是延迟执行，Action类算子是触发执行。
 *          一个application应用程序中有几个Action类算子执行，就有几个job运行。
 *      3、持久化算子
 *
 */
public class SparkWordCount {

    public static void main(String[] args) {

        // Spark配置项
        SparkConf conf = new SparkConf().setAppName("java-wordcount").setMaster("local");
        // java中创建的是JavaSparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 文件路径
        String path = "C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt";
        // 读取文件
        JavaRDD<String> lines = context.textFile(path);

        // 将文件中的每行数据按照空格切割
        // lambda简写
//        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // scala中是map方法，java中使用mapToPair方法
        // lambda简写
//        JavaPairRDD<String, Integer> pairWords = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 先按照key进行分组，在对每个组内key的value值进行聚合
        // lambda简写
//        JavaPairRDD<String, Integer> res = pairWords.reduceByKey((v1, v2) -> v1 + v2);
        JavaPairRDD<String, Integer> res = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 遍历打印
        // lambda简写
//        res.foreach(tuple2 -> System.out.println(tuple2));
        res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });


    }
}

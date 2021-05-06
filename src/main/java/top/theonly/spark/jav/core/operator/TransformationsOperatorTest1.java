package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Transformation 转换算子
 *     Transformations类算子是一类算子（函数）叫做转换算子
 *     如map,flatMap,reduceByKey等。
 *     Transformations算子是延迟执行，也叫懒加载执行。必须有触发算子触发才会执行
 *
 * filter
 *     过滤符合条件的记录数，true保留，false过滤掉。
 *
 * map
 *     将一个RDD中的每个数据项，通过map中的函数映射变为一个新的元素。
 *     特点：输入一条，输出一条数据。
 *
 * flatMap
 *     先map后flat。与map类似，每个输入项可以映射为0到多个输出项。
 *
 * sample
 *     随机抽样算子，根据传进去的小数按比例进行又放回或者无放回的抽样。
 *
 * reduceByKey
 *     将相同的Key根据相应的逻辑进行处理。
 *
 * sortByKey/sortBy
 *     作用在K,V格式的RDD上，对key进行升序或者降序排序。
 */
public class TransformationsOperatorTest1 {

    public static void main(String[] args) {

        // Spark配置项
        SparkConf conf = new SparkConf().setAppName("java-wordcount").setMaster("local");
        // java中创建的是JavaSparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 文件路径
        String path = "C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt";
        // 读取文件
        JavaRDD<String> lines = context.textFile(path);

        /**
         * 转换算子 sample
         * 抽样
         * 参数：
         *  withReplacement：有无放回抽样；true：有放回（可能会抽到同一条数据），false：无放回
         *  fraction：比例因子，小于1的Double类型；是不精确的
         *  seed：种子，Long类型；给定相同种子后，数据每次抽样得到的结果不变
         */
//        lines.sample(true, 0.5);
//        lines.sample(false, 0.5, 100L);

        /**
         * 转换算子 flatMap
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        /**
         * 转换算子 mapToPair
         */
        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        /**
         * 转换算子 reduceByKey
         */
        JavaPairRDD<String, Integer> ret = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        /**
         * 转换算子 sortByKey
         * 排序
         */
//        JavaPairRDD<String, Integer> res = ret.sortByKey(false);

        /**
         * 转换算子 filter
         * 排序
         */
        JavaPairRDD<String, Integer> res = ret.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2._2 > 20;
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

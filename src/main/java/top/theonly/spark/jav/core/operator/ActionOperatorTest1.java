package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * Action 触发算子
 *     Action类算子也是一类算子（函数）叫做行动算子，如foreach,collect，count等
 *     触发Transformations类算子执行
 *     Transformations类算子是延迟执行，Action类算子是触发执行。
 *     一个application应用程序中有几个Action类算子执行，就有几个job运行。
 *
 *
 * count
 * 返回数据集中的元素数。会在结果计算完成后回收到Driver端。
 *
 * take(n)
 * 返回一个包含数据集前n个元素的集合。
 *
 * first
 * first=take(1),返回数据集中的第一个元素。
 *
 * foreach
 * 循环遍历数据集中的每个元素，运行相应的逻辑。
 *
 * collect
 * 将计算结果回收到Driver端。
 */
public class ActionOperatorTest1 {

    public static void main(String[] args) {
        // Spark配置项
        SparkConf conf = new SparkConf().setAppName("java-wordcount").setMaster("local");
        // java中创建的是JavaSparkContext
        JavaSparkContext context = new JavaSparkContext(conf);
        // 文件路径
        String path = "C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt";
        // 读取文件
        JavaRDD<String> lines = context.textFile(path);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairWords = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> res = pairWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });


        /**
         * 行动（触发）算子foreach
         */
        res.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });

        /**
         * 行动（触发）算子count()
         */
//        long count = res.count();

        /**
         * 行动（触发）算子collectAsMap()
         */
//        Map<String, Integer> map = res.collectAsMap();

        /**
         * 行动（触发）算子collect()
         */
//        List<Tuple2<String, Integer>> collect = res.collect();

        /**
         * 行动（触发）算子take(int num)
         */
//        List<Tuple2<String, Integer>> take = res.take(2);

        /**
         * 行动（触发）算子first()
         */
//        Tuple2<String, Integer> first = res.first();

    }
}

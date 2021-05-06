package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformations 类算子
 *
 *  groupByKey
 *      作用在K，V格式的RDD上。根据Key进行分组。作用在（K，V），返回（K，Iterable <V>）。
 *
 *  zip
 *      将两个RDD中的元素（KV格式/非KV格式）变成一个KV格式的RDD,两个RDD的每个分区元素个数必须相同。
 *
 *  zipWithIndex
 *      该函数将RDD中的元素和这个元素在RDD中的索引号（从0开始）组合成（K,V）对。
 *
 *  mapValues
 *      针对K,V格式的RDD,该函数对K,V格式RDD中的value做操作，返回是K,V格式的RDD.
 */
public class TransformationsOperatorTest5 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("TransformationsOperatorTest4");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> rdd1 = context.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("b", 3),
                new Tuple2<String, Integer>("a", 6),
                new Tuple2<String, Integer>("c", 2),
                new Tuple2<String, Integer>("b", 9)));
        /**
         * groupByKey
         *
         * (a,[1, 6])
         * (b,[3, 9])
         * (c,[2])
         */
        JavaPairRDD<String, Iterable<Integer>> gbkRDD = rdd1.groupByKey();
        gbkRDD.foreach(tp -> System.out.println(tp));

        /**
         * mapValues
         *
         * (a,2)
         * (b,6)
         * (a,12)
         * (c,4)
         * (b,18)
         */
        JavaPairRDD<String, Object> mvRDD = rdd1.mapValues(new Function<Integer, Object>() {
            @Override
            public Object call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });
        mvRDD.foreach(tp-> System.out.println(tp));

        JavaRDD<String> rdd2 = context.parallelize(Arrays.asList(
                "a", "b", "c", "d"));
        JavaRDD<Integer> rdd3 = context.parallelize(Arrays.asList(
                1,2,3,4));
        /**
         * zip
         *
         * (a,1)
         * (b,2)
         * (c,3)
         * (d,4)
         */
        JavaPairRDD<String, Integer> zipRDD = rdd2.zip(rdd3);
        zipRDD.foreach(tp-> System.out.println(tp));

        /**
         * zipWithIndex
         *
         * (a,0)
         * (b,1)
         * (c,2)
         * (d,3)
         */
        JavaPairRDD<String, Long> zipWithIndexRDD = rdd2.zipWithIndex();
        zipWithIndexRDD.foreach(tp-> System.out.println(tp));




    }
}

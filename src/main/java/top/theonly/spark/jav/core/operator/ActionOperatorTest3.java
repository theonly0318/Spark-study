package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Action 触发算子
 *
 *  countByKey
 *      作用到K,V格式的RDD上，根据Key计数相同Key的数据集元素。
 *
 *  countByValue
 *      根据数据集每个元素相同的内容来计数。返回相同内容的元素对应的条数。
 */
public class ActionOperatorTest3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("ActionOperatorTest2");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> rdd1 = context.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 2),
                new Tuple2<>("b", 5),
                new Tuple2<>("c", 3),
                new Tuple2<>("c", 3),
                new Tuple2<>("d", 4)));
        /**
         * countByKey
         * d---1
         * a---1
         * b---2
         * c---2
         */
        Map<String, Long> map = rdd1.countByKey();
        map.forEach((k, v)-> System.out.println(k+"---"+v));

        /**
         * countByValue
         *
         * (c,3)---2
         * (b,2)---1
         * (b,5)---1
         * (d,4)---1
         * (a,1)---1
         */
        Map<Tuple2<String, Integer>, Long> map2 = rdd1.countByValue();
        map2.forEach((k, v)-> System.out.println(k+"---"+v));


    }
}

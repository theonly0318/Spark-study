package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformations 类算子
 *
 *  mapPartitionWithIndex
 *      类似于mapPartitions,除此之外还会携带分区的索引值。
 *
 *  repartition
 *      增加或减少分区。会产生shuffle。（多个分区分到一个分区不会产生shuffle）
 *
 *  coalesce
 *      coalesce常用来减少分区，第二个参数是减少分区的过程中是否产生shuffle。
 *      true为产生shuffle，false不产生shuffle。默认是false。
 *      如果coalesce设置的分区数比原来的RDD的分区数还多的话，第二个参数设置为false不会起作用，如果设置成true，效果和repartition一样。
 *      即repartition(numPartitions) = coalesce(numPartitions,true)
 */
public class TransformationsOperatorTest4 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("TransformationsOperatorTest4");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.parallelize(Arrays.asList(
                "hello1", "hello2", "hello3", "hello4",
                "hello5", "hello6", "hello7", "hello8",
                "hello9", "hello10", "hello11", "hello12"), 3);

        /**
         * mapPartitionsWithIndex(Function2<Integer, Iterator<String>, Iterator<String>>(), preservesPartitioning)
         *      preservesPartitioning：是否保留原RDD的分区策略（哈希）
         */
        JavaRDD<String> mpRDD1 = rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    list.add("rdd partition num is 【" + index + "】, value is " + next);
                }
                return list.iterator();
            }
        }, true);
//
//        List<String> collect = mpRDD1.collect();
//        collect.forEach(s -> System.out.println(s));

        /**
         * repartition(numPartitions: Int)
         */
//        JavaRDD<String> repartitionRDD = rdd2.repartition(4);
//        JavaRDD<String> mpRDD2 = repartitionRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
//            @Override
//            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
//                List<String> list = new ArrayList<>();
//                while (iterator.hasNext()) {
//                    String next = iterator.next();
//                    list.add("rdd partition num is 【" + index + "】, value is " + next);
//                }
//                return list.iterator();
//            }
//        }, true);
//        List<String> collect1 = mpRDD2.collect();
//        collect1.forEach(s -> System.out.println(s));

        /**
         * coalesce(numPartitions: Int, shuffle: Boolean)
         *      shuffle：是否产生shuffle，默认为false
         *
         *  coalesce(numPartitions,true) = repartition(numPartitions)
         */
//        JavaRDD<String> coalesceRDD = mpRDD1.coalesce(4, true);
//        JavaRDD<String> coalesceRDD = mpRDD1.coalesce(4, true);
//        JavaRDD<String> coalesceRDD = mpRDD1.coalesce(2, true);
        JavaRDD<String> coalesceRDD = mpRDD1.coalesce(4, false);

        JavaRDD<String> mpRDD3 = coalesceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    list.add("rdd partition num is 【" + index + "】, value is " + next);
                }
                return list.iterator();
            }
        }, true);
        List<String> collect2 = mpRDD3.collect();
        collect2.forEach(s -> System.out.println(s));
    }
}

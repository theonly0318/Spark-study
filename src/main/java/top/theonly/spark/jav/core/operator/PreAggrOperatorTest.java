package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * map端预聚合算子
 *
 * reduceByKey
 *
 * aggregateByKey
 *
 * combineByKey
 *
 * @author theonly
 */
public class PreAggrOperatorTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("PreAggrOperatorTest");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> rdd = sparkContext.parallelizePairs(Arrays.asList(
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1),
                new Tuple2<>("a", 1),
                new Tuple2<>("b", 1),
                new Tuple2<>("c", 1)
        ), 3);

        /**
         * partition num is 0, value is (a,1)
         * partition num is 0, value is (b,1)
         * partition num is 0, value is (c,1)
         * partition num is 0, value is (a,1)
         * partition num is 1, value is (b,1)
         * partition num is 1, value is (c,1)
         * partition num is 1, value is (a,1)
         * partition num is 1, value is (b,1)
         * partition num is 2, value is (c,1)
         * partition num is 2, value is (a,1)
         * partition num is 2, value is (b,1)
         * partition num is 2, value is (c,1)
         */
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<String, Integer>>, Iterator<Tuple2<String, Integer>>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer index, Iterator<Tuple2<String, Integer>> iterator) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<String, Integer> next = iterator.next();
                    System.out.println("partition num is " + index + ", value is " + next);
                    list.add(next);
                }
                return list.iterator();
            }
        }, true).count();

        /**
         * reduceByKey
         */
        JavaPairRDD<String, Integer> rdd2 = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * aggregateByKey
         *
         * partition-0
         * (a,1)              (a,2)         (a,2+1+1)     (a,4)
         * (b,1)         =>   (b,2)     =>  (b,2+1)       (b,3)
         * (c,1)              (c,2)         (c,2+1)       (c,3)
         * (a,1)
         * partition-1
         * (b,1)              (a,2)         (a,2+1)       (a,3)         reduce        (a,4+3+3)       (a,10)
         * (c,1)         =>   (b,2)     =>  (b,2+1+1)     (b,4)       ===========>    (b,3+4+3)       (b,10)
         * (a,1)              (c,2)         (c,2+1)       (c,3)                       (c,3+3+4)       (c,10)
         * (b,1)
         * partition-2
         * (c,1)              (a,2)         (a,2+1)       (a,3)
         * (a,1)         =>   (b,2)     =>  (b,2+1)       (b,3)
         * (b,1)              (c,2)         (c,2+1+1)     (c,4)
         * (c,1)
         */
        JavaPairRDD<String, Integer> rdd3 = rdd.aggregateByKey(
                // (a,2),(b,2),(c,2)
                2,
                // 作用在分区上 v1: 2   v2: 1  return 3
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                },
                // 作用在reduce端
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        /**
         * combineByKey
         */
        JavaPairRDD<String, Integer> rdd4 = rdd.combineByKey(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });



        rdd3.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                System.out.println(tuple2);
            }
        });


    }
}

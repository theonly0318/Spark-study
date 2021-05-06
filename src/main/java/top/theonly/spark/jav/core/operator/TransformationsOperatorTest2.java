package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Option;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 补充Transformation算子
 *
 *  join,leftOuterJoin,rightOuterJoin,fullOuterJoin
 *    作用在K,V格式的RDD上。根据K进行连接，对（K,V）join(K,W)返回（K,(V,W)）
 *     join后的分区数与父RDD分区数多的那一个相同。
 */
public class TransformationsOperatorTest2 {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        conf.setMaster("local");
        conf.setAppName("TransformationsOperatorTest2");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaPairRDD<String, Integer> ageRDD = context.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 19),
                new Tuple2<String, Integer>("lisi", 20),
                new Tuple2<String, Integer>("wangwu", 21),
                new Tuple2<String, Integer>("zhaoliu", 22)
        ));

        JavaPairRDD<String, Integer> scoreRDD = context.parallelizePairs(Arrays.asList(
                new Tuple2<String, Integer>("zhangsan", 96),
                new Tuple2<String, Integer>("lisi", 77),
                new Tuple2<String, Integer>("wangwu", 85),
                new Tuple2<String, Integer>("maqi", 89)
        ));

        /**
         * join：
         *    只对相同key进行join，不同key舍去
         */
//        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = ageRDD.join(scoreRDD);
//
//        joinRDD.foreach(tp -> {
//            String name = tp._1;
//            Integer age = tp._2._1;
//            Integer score = tp._2._2;
//            System.out.println("name="+name+", age="+age+", score="+score);
//        });


        /**
         * leftOuterJoin：
         *    以左边的RDD的key为准
         */
//        JavaPairRDD<String, Tuple2<Integer, Optional<Integer>>> leftJoinRDD = ageRDD.leftOuterJoin(scoreRDD);
//
//        leftJoinRDD.foreach(tp -> {
//            String name = tp._1;
//            Integer age = tp._2._1;
//            Optional<Integer> optional = tp._2._2;
//            if (optional.isPresent()) {
//                System.out.println("name="+name+", age="+age+", score="+optional.get());
//            } else {
//                System.out.println("name="+name+", age="+age+", score=None");
//            }
//        });

        /**
         * rightOuterJoin：
         *    以右边的RDD的key为准
         */
//        JavaPairRDD<String, Tuple2<Optional<Integer>, Integer>> rightJoinRDD = ageRDD.rightOuterJoin(scoreRDD);
//        rightJoinRDD.foreach(tp -> {
//            String name = tp._1;
//            Optional<Integer> optional = tp._2._1;
//            Integer score = tp._2._2;
//            if (optional.isPresent()) {
//                System.out.println("name="+name+", age="+optional.get()+", score="+score);
//            } else {
//                System.out.println("name="+name+", age=None, score="+score);
//            }
//        });


        /**
         * fullOuterJoin：
         *    两边的RDD的key都会包含
         */
        JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<Integer>>> fullJoinRDD = ageRDD.fullOuterJoin(scoreRDD);
        fullJoinRDD.foreach(tp-> {
            String name = tp._1;
            Optional<Integer> ageOptional = tp._2._1;
            Optional<Integer> scoreOptional = tp._2._2;
            System.out.println("name="+name+", age=" + (ageOptional.isPresent()? ageOptional.get(): "None")
                    +", score=" + (scoreOptional.isPresent() ? scoreOptional.get() : "None"));
        });


    }
}

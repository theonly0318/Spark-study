package top.theonly.spark.jav.core.cases.pvuv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Map;

public class PvAndUvCase2 {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("PVCase");

        JavaSparkContext context = new JavaSparkContext(conf);
        // 120.190.182.66	海南	2021-04-23	1619180612897	4698515425423631796	www.suning.com	Login
        JavaRDD<String> rdd1 = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\pvuvdata");

        // lambda表达式
//        Map<String, Integer> pvMap =
//                // 网站url每出现一次就记一次数，将网站url作为key，value为1
//                rdd1.mapToPair(line -> new Tuple2<>(line.split("\t")[5], 1))
//                // 通过key进行聚合，将次数相加
//                .reduceByKey((v1, v2) -> v1 + v2)
//                // 将key，value换位置，便于排序
//                .mapToPair(tp -> tp.swap())
//                // 对PV量降序排序
//                .sortByKey(false)
//                // 还原key，value
//                .mapToPair(tp -> tp.swap())
//                // 将结果RDD收集为map
//                .collectAsMap();

//        Map<String, Integer> uvMap = rdd1.map(line -> line.split("\t")[4] + "_" + line.split("\t")[5])
//                .distinct()
//                .mapToPair(line -> new Tuple2<>(line.split("_")[1], 1))
//                .reduceByKey((v1, v2) -> v1 + v2)
//                .mapToPair(tuple2 -> tuple2.swap())
//                .sortByKey(false)
//                .mapToPair(tuple2 -> tuple2.swap())
//                .collectAsMap();

        /**
         * 计算PV
         */
        JavaPairRDD<String, Integer> pvRDD = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                // 将网站url作为key，value为1
                // 网站url每出现一次就记一次数
                return new Tuple2<>(s.split("\t")[5], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            // 通过key进行聚合，将次数相加
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            // 将key，value换位置，便于排序
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        }).sortByKey(
            // 对PV量降序排序
            false
        ).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            // 还原key，value
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        // 将结果RDD收集为map
        Map<String, Integer> pvMap = pvRDD.collectAsMap();
        // 遍历
        pvMap.forEach((website, pvCount) -> System.out.println("网站："+website+"，PV量："+pvCount));


        /**
         * 计算UV：对用户id进行uv计算
         */
        JavaPairRDD<String, Integer> uvRDD = rdd1.map(new Function<String, String>() {
            // 对每一行进行处理，userId_website
            @Override
            public String call(String line) throws Exception {
                return line.split("\t")[4] + "_" + line.split("\t")[5];
            }
        }).distinct() // 去重
        .mapToPair(new PairFunction<String, String, Integer>() {
            // 去重后在对website进行计数，出现一次，计数1
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split("_")[1], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            // 通过key进行聚合，将次数相加
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            // 将key，value换位置，便于排序
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.swap();
            }
        }).sortByKey(
                // 对UV量降序排序
                false
        ).mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            // 还原key，value
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return tuple2.swap();
            }
        });

        // 将结果RDD收集为map
        Map<String, Integer> uvMap = uvRDD.collectAsMap();
        // 遍历
        uvMap.forEach((website, pvCount) -> System.out.println("网站："+website+"，UV量："+pvCount));
    }
}

package top.theonly.spark.jav.core.cases.pvuv;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Map;

/**
 * 计算网站 PV、UV的小case
 * @author guo'shi'hua
 */
public class PvAndUvCase {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("PVCase");

        JavaSparkContext context = new JavaSparkContext(conf);
        // 120.190.182.66	海南	2021-04-23	1619180612897	4698515425423631796	www.suning.com	Login
        JavaRDD<String> rdd1 = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\pvuvdata");

        // 将每行数据按 \t 切分，生成tuple， _1为网站url，_2为用户id
        JavaPairRDD<String, String> rdd2 = rdd1.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String website = line.split("\t")[5];
                String userId = line.split("\t")[4];
                return new Tuple2<>(website, userId);
            }
        });

        // 计算PV，对key即网站url计数
        Map<String, Long> pvMap = rdd2.countByKey();
        pvMap.forEach((website, count) -> {
            System.out.println("网站："+website+"，PV量："+count);
        });

        // 计算UV，去重后 对key即网站url计数
        JavaPairRDD<String, String> rdd3 = rdd2.distinct();
        Map<String, Long> uvMap = rdd3.countByKey();
        uvMap.forEach((website, count) -> {
            System.out.println("网站："+website+"，UV量："+count);
        });


    }
}

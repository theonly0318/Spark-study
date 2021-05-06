package top.theonly.spark.jav.core.partition;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 自定义分区器测试
 *  rdd.partitionBy(new MyPartitioner())
 *
 * @author theonly
 */
public class SelfDefPartitionerTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SelfDefPartitonerTest");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaPairRDD<Integer, String> rdd = sparkContext.parallelizePairs(Arrays.asList(
                new Tuple2<>(1, "zhangsan"),
                new Tuple2<>(2, "lisi"),
                new Tuple2<>(3, "wangwu"),
                new Tuple2<>(4, "zhaoliu"),
                new Tuple2<>(5, "tianqi"),
                new Tuple2<>(6, "maba")
        ));

        // 对分区进行map操作，打印分区号和value值
        rdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, String>>, Iterator<Tuple2<Integer, String>>>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Integer index, Iterator<Tuple2<Integer, String>> iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println("初始：partition num = " + index + ", value = " + next);
                    list.add(next);
                }
                return list.iterator();
            }
        }, true).count();

        // 对rdd根据自定义分区器重新分区
        JavaPairRDD<Integer, String> rdd2 = rdd.partitionBy(new MyPartitioner());

        // 对分区进行map操作，打印分区号和value值
        rdd2.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, String>>, Iterator<Tuple2<Integer, String>>>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Integer index, Iterator<Tuple2<Integer, String>> iterator) throws Exception {
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    Tuple2<Integer, String> next = iterator.next();
                    System.out.println("自定义分区后：partition num = " + index + ", value = " + next);
                    list.add(next);
                }
                return list.iterator();
            }
        }, true).count();
    }
}

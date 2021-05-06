package top.theonly.spark.jav.core.cases.topn;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * 分组 topN
 * @author theonly
 */
public class TopNCase {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SecondSortCase");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\topn.txt");

        JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line.split(" ")[0], Integer.valueOf(line.split(" ")[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupRDD = pairRDD.groupByKey();

        /**
         *  原生集合方式
         *
         *  如果数据过多，会发生OOM
         */
//        JavaPairRDD<String, List<Integer>> pairRDD2 = groupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, List<Integer>>() {
//            @Override
//            public Tuple2<String, List<Integer>> call(Tuple2<String, Iterable<Integer>> tp2) throws Exception {
//                Iterator<Integer> iterator = tp2._2.iterator();
//                // 将每个分组的数据转成list（如果数据过多，会发生OOM）
//                List<Integer> list = IteratorUtils.toList(iterator);
//                // 排序（倒序）
//                Collections.sort(list, new Comparator<Integer>() {
//                    @Override
//                    public int compare(Integer o1, Integer o2) {
//                        return o2 - o1;
//                    }
//                });
//                // 去除前三条
//                List<Integer> top3List = list.subList(0, 3);
//                return new Tuple2<>(tp2._1, top3List);
//            }
//        });
//
//        pairRDD2.foreach(new VoidFunction<Tuple2<String, List<Integer>>>() {
//            @Override
//            public void call(Tuple2<String, List<Integer>> tp2) throws Exception {
//                System.out.println(tp2);
//            }
//        });

        /**
         * 定长数组方式
         */
        JavaPairRDD<String, Integer[]> pairRDD2 = groupRDD.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Integer[]>() {
            @Override
            public Tuple2<String, Integer[]> call(Tuple2<String, Iterable<Integer>> tp2) throws Exception {
                Iterator<Integer> iterator = tp2._2.iterator();
                Integer[] array = new Integer[3];

                while (iterator.hasNext()) {
                    Integer next = iterator.next();
                    for (int i = 0; i < array.length; i++) {
                        if (array[i] == null) {
                            array[i] = next;
                            break;
                        } else if (next > array[i]) {
                            for (int j = array.length - 1; j > i; j--) {
                                array[j] = array[j-1];
                            }
                            array[i] = next;
                            break;
                        }
                    }
                }
                return new Tuple2<>(tp2._1, array);
            }
        });
        pairRDD2.foreach(tp -> {
            System.out.println(tp._1);
            for (Integer integer : tp._2) {
                System.out.println(integer);
            }
        });



    }
}

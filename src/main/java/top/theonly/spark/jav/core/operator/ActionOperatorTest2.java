package top.theonly.spark.jav.core.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Action 触发算子
 *
 *  foreachPartition
 *      遍历的数据是每个partition的数据。
 *
 *  collectAsMap
 *      对K,V格式的RDD数据回收转换成Map<K,V>
 *
 *  takeSample(boolean,num，seed)
 *      takeSample可以对RDD中的数据随机获取num个，第一个参数是有无放回，第二个参数是随机获取几个元素，第三个参数如果固定，那么每次获取的数据固定。
 *
 *  top(num)
 *      对RDD中的所有元素进行由大到小排序，获取前num个元素返回。
 *
 *  takeOrdered(num)
 *      对RDD中的所有元素进行由小到大的排序，获取前num个元素返回。
 */
public class ActionOperatorTest2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("ActionOperatorTest2");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.parallelize(Arrays.asList(
                "hello1", "hello2", "hello3", "hello4",
                "hello5", "hello6", "hello7", "hello8",
                "hello9", "hello10", "hello11", "hello12"), 3);

        /**
         * foreachPartition
         */
        rdd.foreachPartition(iterator -> {
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        });

//        rdd.foreachPartition(new VoidFunction<Iterator<String>>() {
//            @Override
//            public void call(Iterator<String> iterator) throws Exception {
//                while (iterator.hasNext()) {
//                    System.out.println(iterator.next());
//                }
//            }
//        });

        JavaPairRDD<String, Integer> rdd2 = context.parallelizePairs(Arrays.asList(
                new Tuple2<>("张三", 22),
                new Tuple2<>("李四", 23),
                new Tuple2<>("王五", 24),
                new Tuple2<>("赵六", 25)));
        /**
         * collectAsMap
         */
        Map<String, Integer> map = rdd2.collectAsMap();
        map.forEach((k,v) -> System.out.println("key is " + k + ", value is " +v));

        /**
         * 抽样
         * takeSample(withReplacement: Boolean, num: Int, seed: Long)
         *  withReplacement：是否替换（有无放回），true：有放回，可能抽到相同数据
         *  num：抽取元素个数
         *  seed：种子，相同种子，多次抽取的数据是一致的
         */
        List<String> list = rdd.takeSample(true, 10, 100);
        list.forEach(s-> System.out.println(s));

        /**
         * 对RDD中的所有元素进行由大到小排序，获取前num个元素返回。
         * top(num: Int, comp: Comparator[T])
         * 可以自定义比较器
         */
        List<String> top = rdd.top(5);
        top.forEach(s-> System.out.println(s));

        // 异常：org.apache.spark.SparkException: Task not serializable
//        List<Tuple2<String, Integer>> top1 = rdd2.top(2, new Comparator<Tuple2<String, Integer>>() {
//            @Override
//            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
//                return (o1._2 >= o2._2) ? 0 : 1;
//            }
//        });
//        top1.forEach(tp -> System.out.println(tp));

        /**
         * takeOrdered(num: Int, comp: Comparator[T])
         *  对RDD中的所有元素进行由小到大的排序，获取前num个元素返回。
         *  可以自定义比较器
         */
        List<String> list1 = rdd.takeOrdered(5);
        list1.forEach(s-> System.out.println(s));
    }
}

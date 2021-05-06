package top.theonly.spark.jav.core.broadcast;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.List;

/**
 * 广播变量
 *          JavaSparkContext.broadcast(T value)
 *
 *    当Executor端使用到了Driver端的变量时，可以使用广播变量来减少Executor端的内存使用
 *
 *    注意问题：
 *      1)、不能将RDD广播出去，可以将RDD收集到的结果回收到Driver端之后再广播
 *      2)、广播变量只能在Driver端定义，在Executor端不能修改广播变量的值
 */
public class BroadcastTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("BroadcastTest");

        JavaSparkContext context = new JavaSparkContext(conf);

        // 变量
        List<String> list = new ArrayList<>();
        list.add("hello spark");
        // 将变量广播到Executor端
        Broadcast<List<String>> broadcast = context.broadcast(list);

        JavaRDD<String> rdd = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt");

        JavaRDD<String> filterMap = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                // 获取广播变量的值
                List<String> value = broadcast.value();
                return value.contains(line);
            }
        });

        filterMap.foreach(line -> System.out.println(line));

    }

}

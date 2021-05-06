package top.theonly.spark.jav.core.accumulator;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;

import java.util.ArrayList;
import java.util.List;

/**
 * 累加器
 *      SparkContext.longAccumulator(String name)
 *    累加器是分布式中的统筹变量，Spark分布式计算过程如果有计数需求，要使用累加器
 *
 *    注意事项：
 *      累加器只能在Driver端定义，在Executor端使用、更新
 *
 * @author theonly
 */
public class AccumulatorTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SecondSortCase");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt");

        // 获取Scala中的SparkContext
        SparkContext sc = context.sc();
        // 创建累加器
        LongAccumulator iAcc = sc.longAccumulator("acc_i");

        JavaRDD<String> mapRDD = rdd.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                // 累加器计数+1
                iAcc.add(1);
                return line;
            }
        });
        mapRDD.collect();

        //acc acc_i value is 66
        System.out.println("acc " + iAcc.name().get() + " value is " + iAcc.value());
        //66
        System.out.println(iAcc.count());

    }
}

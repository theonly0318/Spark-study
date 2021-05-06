package top.theonly.spark.jav.streaming.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * transform：Java中为 transformToPair
 *  可以获取DStream中的RDD，对RDD使用RDD的算子进行操作，要求返回RDD，最后将RDD封装为DStream
 *  transform算子内获取的RDD的算子外的代码是在Driver端执行的。可以动态改变广播变量
 *
 * @author theonly
 */
public class TransformTest {

    public static void main(String[] args) throws InterruptedException {
        // 创建SparkConf
        SparkConf conf = new SparkConf();
        // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
        conf.setMaster("local[2]");
        // 设置应用程序名称
        conf.setAppName("SparkStreamingReadSocket");
        // 创建SparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        // 创建StreamingContext（java中为JavaStreamingContext）
        // 参数1：JavaSparkContext，参数2：batchDuration : org.apache.spark.streaming.Duration，多长时间生成一批次数据
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(5));

        // 参数1也可以使用SparkConf对象，但该应用程序中不能出现SparkContext对象
//        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        // 从socket服务中读取数据流（指定socket服务的主机名和端口），结果封装为ReceiverInputDStream
        JavaReceiverInputDStream<String> linesDS = streamingContext.socketTextStream("node0", 9999);

        /**
         * transformToPair
         */
        JavaPairDStream<String, Integer> resDS = linesDS.transformToPair(new Function<JavaRDD<String>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaRDD<String> lineRDD) throws Exception {
                lineRDD.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public Iterator<String> call(String s) throws Exception {
                        return null;
                    }
                });
                return lineRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(word -> new Tuple2<>(word, 1))
                        .reduceByKey((v1, v2) -> v1 + v2);
            }
        });

        resDS.print();

        // 开启Streaming程序
        streamingContext.start();
        // 等待终止，不间断运行
        streamingContext.awaitTermination();
    }
}

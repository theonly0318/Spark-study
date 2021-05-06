package top.theonly.spark.jav.streaming.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * foreachRDD
 *      output operation算子,必须对抽取出来的RDD执行action类算子，代码才能执行。
 *      可以获取DStream中的RDD，对RDD使用RDD的算子进行操作
 *      foreachRDD算子内获取的RDD的算子外的代码是在Driver端执行的。可以动态改变广播变量
 * @author theonly
 */
public class ForeachRDDTest {

    public static void main(String[] args) throws InterruptedException {
        // 创建SparkConf
        SparkConf conf = new SparkConf();
        // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
        conf.setMaster("local[2]");
        // 设置应用程序名称
        conf.setAppName("ForeachRDDTest");
        // 创建SparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        sparkContext.setLogLevel("ERROR");
        // 创建StreamingContext（java中为JavaStreamingContext）
        // 参数1：JavaSparkContext，参数2：batchDuration : org.apache.spark.streaming.Duration，多长时间生成一批次数据
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, Durations.seconds(5));

        // 参数1也可以使用SparkConf对象，但该应用程序中不能出现SparkContext对象
//        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        // 从socket服务中读取数据流（指定socket服务的主机名和端口），结果封装为ReceiverInputDStream
        JavaReceiverInputDStream<String> linesDS = streamingContext.socketTextStream("node0", 9999);


        // 将每行数据按空格切分，每行一个单词，生成新的DStream
        JavaDStream<String> wordDS = linesDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });
        // 对每行的单词的出现次数计数1
        JavaPairDStream<String, Integer> pairMapDS = wordDS.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<>(line, 1);
            }
        });
        // 对tuple2中key即单词进行分组，将次数累加
        JavaPairDStream<String, Integer> resultDS = pairMapDS.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        /**
         * foreachRDD
         */
        resultDS.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
                rdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                    @Override
                    public void call(Tuple2<String, Integer> tuple2) throws Exception {
                        System.out.println(tuple2._1+"#"+tuple2._2);
                    }
                });
            }
        });



        // 开启Streaming程序
        streamingContext.start();
        // 等待终止，不间断运行
        streamingContext.awaitTermination();
    }
}

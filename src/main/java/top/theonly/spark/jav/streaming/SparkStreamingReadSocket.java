package top.theonly.spark.jav.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 读取Socket数据，进行wordcount
 * 1、node0服务器开启一个socket服务 nc -lk 9999    yum install -y nc
 * 2、启动，运行main方法
 * 3、在node0的socket服务中发送消息，格式：hello spark
 * 4、检测控制台输出
 * @author theonly
 */
public class SparkStreamingReadSocket {

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

        // 控制台打印
        resultDS.print();
        // 开启Streaming程序
        streamingContext.start();
        // 等待终止，不间断运行
        streamingContext.awaitTermination();
//        boolean b = streamingContext.awaitTerminationOrTimeout(5000);

        // 停止streaming程序，调用后，不能在执行 streamingContext.start() 来启动streaming程序
        // 参数：true：在回收StreamingContext对象后将SparkContext也回收
        //      false：默认值，在回收StreamingContext对象后不会将SparkContext回收
        streamingContext.stop();
    }
}

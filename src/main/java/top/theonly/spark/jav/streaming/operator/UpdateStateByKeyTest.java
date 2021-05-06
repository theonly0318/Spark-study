package top.theonly.spark.jav.streaming.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
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
import java.util.List;

/**
 * updateStateByKey
 *      transformation算子
 *      updateStateByKey作用：
 *          1)为SparkStreaming中每一个Key维护一份state状态，state类型可以是任意类型的，可以是一个自定义的对象，
 *              更新函数也可以是自定义的。
 *          2)通过更新函数对该key的状态不断更新，对于每个新的batch而言，
 *              SparkStreaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新。
 *
 *    使用到updateStateByKey要开启checkpoint机制和功能
 *
 *    多久会将内存中的数据写入到磁盘一份？
 *          如果batchInterval设置的时间小于10秒，那么10秒写入磁盘一份。
 *          如果batchInterval设置的时间大于10秒，那么就会batchInterval时间间隔写入磁盘一份。
 */
public class UpdateStateByKeyTest {
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

        // 设置checkpoint目录
        streamingContext.checkpoint("./data/streaming/ck");

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

        /**
         * updateStateByKey
         */
        JavaPairDStream<String, Integer> resDS = pairMapDS.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> list, Optional<Integer> optional) throws Exception {
                // list:{1,1,1}
                Integer preValue = optional.isPresent() ? optional.get() : 0;
                for (Integer i : list) {
                    preValue += i;
                }
                return Optional.of(preValue);
            }
        });
        resDS.print();

        // 开启Streaming程序
        streamingContext.start();
        // 等待终止，不间断运行
        streamingContext.awaitTermination();
    }
}

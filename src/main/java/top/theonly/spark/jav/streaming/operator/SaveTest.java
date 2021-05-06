package top.theonly.spark.jav.streaming.operator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import scala.Tuple2;

import java.util.Arrays;

/**
 * DStreams 上的输出操作
 *
 *  输出操作允许将 DStream 的数据推送到外部系统，如数据库或文件系统。
 *  由于输出操作实际上允许外部系统使用变换后的数据，所以它们触发所有 DStream 变换的实际执行（类似于RDD的动作）。
 *  目前，定义了以下输出操作：
 *      print()
 *      foreachRDD(func)
 *      // 以下都是存储在Hadoop的hdfs中
 *      saveAsTextFiles(prefix, [suffix])
 *      saveAsObjectFiles(prefix, [suffix])
 *      saveAsHadoopFiles(prefix, [suffix])
 *
 * @author theonly
 */
public class SaveTest {

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

        // 切割，计数，聚合统计
        JavaPairDStream<String, Integer> pairMapDS =
                linesDS.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(word -> new Tuple2<>(word, 1))
                        .reduceByKey((v1, v2) -> v1+v2);

        /**
         * save
         */
        saveAsTextFiles(pairMapDS.dstream());
//        saveAsObjectFiles(pairMapDS.dstream());
//        saveAsHadoopFiles(pairMapDS);
//        saveAsNewAPIHadoopFiles(pairMapDS);

        // 开启Streaming程序
        streamingContext.start();
        // 等待终止，不间断运行
        streamingContext.awaitTermination();
    }

    /**
     * saveAsTextFiles(prefix, suffix)
     * 将此 DStream 的内容另存为文本文件。
     * 每个批处理间隔的文件名是根据 前缀 和 后缀："prefix-TIME_IN_MS[.suffix]" 生成的。
     *  底层是 RDD.rddToPairRDDFunctions.saveAsHadoopFile
     * @param ds
     */
    public static void saveAsTextFiles(DStream ds) {
        ds.saveAsTextFiles("aaa", "txt");
    }

    /**
     * saveAsObjectFiles(prefix, suffix)
     * 将此 DStream 的内容另存为序列化 Java 对象的 SequenceFiles。
     * 每个批处理间隔的文件名是根据 前缀 和 后缀："prefix-TIME_IN_MS[.suffix]" 生成的。
     *  底层是 saveAsHadoopFile
     * @param ds
     */
    public static void saveAsObjectFiles(DStream ds) {
        ds.saveAsObjectFiles("aaa", "txt");
    }
    /**
     *  saveAsHadoopFile(prefix, suffix, keyClass, valueClass, outputFormatClass)
     *  将此 DStream 的内容另存为 Hadoop 文件。
     *  每个批处理间隔的文件名是根据 前缀 和 后缀："prefix-TIME_IN_MS[.suffix]" 生成的。
     * @param ds
     */
    public static void saveAsHadoopFiles(JavaPairDStream ds) {
        ds.saveAsHadoopFiles("aaa", "txt", Text.class, IntWritable.class, TextOutputFormat.class);
    }

    /**
     * saveAsHadoopFiles的新的API
     * @param ds
     */
    public static void saveAsNewAPIHadoopFiles(JavaPairDStream ds) {
        ds.saveAsNewAPIHadoopFiles("aaa", "txt", Text.class, IntWritable.class, TextOutputFormat.class);
    }
}

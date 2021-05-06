package top.theonly.spark.jav.streaming.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Window Operations（窗口操作）
 *       - window length（窗口长度） - 窗口的持续时间
 *       - sliding interval（滑动间隔） - 执行窗口操作的间隔
 *       这两个参数必须是 source DStream 的 batch interval（批间隔）的倍数
 *
 * 1. window(windowLength, slideInterval)
 * 2. countByWindow(windowLength, slideInterval)
 * 3. reduceByWindow(func, windowLength, slideInterval)
 * 4. reduceByKeyAndWindow(func, windowLength, slideInterval, [numTasks])
 * 5. reduceByKeyAndWindow(func, invFunc, windowLength, slideInterval, [numTasks])
 * 6. countByValueAndWindow(windowLength, slideInterval, [numTasks])
 *
 * @author theonly
 */
public class WindowTest {

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

        JavaPairDStream<String, Integer> pairMapDS =
                linesDS.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                        .mapToPair(word -> new Tuple2<>(word, 1));

//        window(pairMapDS);
//        countByWindow(streamingContext, pairMapDS);
//        reduceByKeyAndWindow(pairMapDS);
        reduceByKeyAndWindowInv(streamingContext, pairMapDS);

        // 开启Streaming程序
        streamingContext.start();
        // 等待终止，不间断运行
        streamingContext.awaitTermination();

    }

    /**
     * window(窗口长度, 滑动间隔)
     * @param dStream
     */
    public static void window(JavaPairDStream<String, Integer> dStream) {
        JavaPairDStream<String, Integer> resDS = dStream.window(Durations.seconds(15), Durations.seconds(5));
        resDS.print();
    }

    /**
     * countByWindow(窗口长度, 滑动间隔)
     * 返回值：JavaDStream
     *
     * 请注意，针对该操作的使用必须启用 checkpoint
     * @param streamingContext
     * @param dStream
     */
    public static void countByWindow(JavaStreamingContext streamingContext, JavaPairDStream<String, Integer> dStream) {
        streamingContext.checkpoint("./data/streaming/ck");
        JavaDStream<Long> resDS = dStream.countByWindow(Durations.seconds(15), Durations.seconds(5));
        resDS.print();
    }

    /**
     * reduceByKeyAndWindow
     * reduceByKeyAndWindow(Function2<Integer, Integer, Integer>(), windowDuration: Duration, slideDuration: Duration)
     * @param dStream
     */
    public static void reduceByKeyAndWindow(JavaPairDStream<String, Integer> dStream) {
        JavaPairDStream<String, Integer> resDS = dStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(15), Durations.seconds(5));
        resDS.print();
    }

    /**
     * reduceByKeyAndWindow() 的优化版本
     *  其中使用前一窗口的 reduce 值逐渐计算每个窗口的 reduce值。
     *  这是通过减少进入滑动窗口的新数据，以及 “inverse reducing（逆减）” 离开窗口的旧数据来完成的。
     *
     * 一个例子是当窗口滑动时”添加” 和 “减” keys 的数量。
     *  然而，它仅适用于 “invertible reduce functions（可逆减少函数）”，
     *  即具有相应 “inverse reduce（反向减少）” 函数的 reduce 函数（作为参数 invFunc </ i>）。
     *  像在 reduceByKeyAndWindow 中的那样，reduce 任务的数量可以通过可选参数进行配置。
     *
     *  请注意，针对该操作的使用必须启用 checkpointing.
     *
     * @param streamingContext
     * @param dStream
     */
    public static void reduceByKeyAndWindowInv(JavaStreamingContext streamingContext, JavaPairDStream<String, Integer> dStream) {
        streamingContext.checkpoint("./data/streaming/ck");
        JavaPairDStream<String, Integer> resDS = dStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(15), Durations.seconds(5));
        resDS.print();
    }
}

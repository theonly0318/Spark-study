package top.theonly.spark.sca.streaming.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * Window Operations（窗口操作）
 *       - window length（窗口长度） - 窗口的持续时间
 *       - sliding interval（滑动间隔） - 执行窗口操作的间隔
 * 这两个参数必须是 source DStream 的 batch interval（批间隔）的倍数
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
object WindowTest {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf()
    // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
    conf.setMaster("local[2]")
    // 设置应用程序名称
    conf.setAppName("UpdateStateByKeyTest")
    // 创建SparkContext
    val context = new SparkContext(conf)
    // 设置日志级别
    context.setLogLevel("ERROR")
    // 创建StreamingContext（java中为JavaStreamingContext）
    // 参数1：JavaSparkContext，参数2：batchDuration : org.apache.spark.streaming.Duration，多长时间生成一批次数据
    val streamingContext = new StreamingContext(context, Durations.seconds(5))

    // 参数1也可以使用SparkConf对象，但该应用程序中不能出现SparkContext对象
    //        val streamingContext: StreamingContext = new StreamingContext(conf, Durations.seconds(5));

    // 从socket服务中读取数据流（指定socket服务的主机名和端口），结果封装为ReceiverInputDStream
    val ds = streamingContext.socketTextStream("node0", 9999)
    val ds2: DStream[(String, Int)] =
    // 将每行数据按空格切分，每行一个单词，生成新的DStream
      ds.flatMap(line => line.split(" "))
        // 对每行的单词的出现次数计数1
        .map(word => (word, 1))

    /**
     * window
     */
//    window(ds2);
//    countByWindow(streamingContext, ds2);
//    reduceByKeyAndWindow(ds2);
    reduceByKeyAndWindowInv(streamingContext, ds2);


    // 启动Streaming程序
    streamingContext.start()
    // 等待终止，不间断运行
    streamingContext.awaitTermination()
    //    val bool: Boolean = streamingContext.awaitTerminationOrTimeout(5000)

    // 停止streaming程序，调用后，不能在执行 streamingContext.start() 来启动streaming程序
    // 参数：true：在回收StreamingContext对象后将SparkContext也回收
    //      false：默认值，在回收StreamingContext对象后不会将SparkContext回收
    streamingContext.stop()
  }

  /**
   * window(窗口长度, 滑动间隔)
   * @param ds
   */
  def window(ds: DStream[(String, Int)]):Unit = {
    val resDS: DStream[(String, Int)] = ds.window(Durations.seconds(15), Durations.seconds(5))
    resDS.print()
  }

  /**
   * countByWindow(窗口长度, 滑动间隔)
   * 返回值：JavaDStream
   *
   * 请注意，针对该操作的使用必须启用 checkpoint
   *
   * @param streamingContext
   * @param ds
   */
  def countByWindow(streamingContext: StreamingContext, ds: DStream[(String, Int)]):Unit = {
    streamingContext.checkpoint("./data/streaming/ck2")
    val resDS = ds.countByWindow(Durations.seconds(15), Durations.seconds(5))
    resDS.print()
  }

  /**
   * reduceByWindow(reduceFunc: (T, T) => T, windowDuration: Duration, slideDuration: Duration)
   * @param streamingContext
   * @param ds
   */
  def reduceByWindow(streamingContext: StreamingContext, ds: DStream[(String, Int)]):Unit = {
    streamingContext.checkpoint("./data/streaming/ck2")
    val resDS = ds.reduceByWindow((v1: (String, Int), v2: (String, Int)) => {return null},
      Durations.seconds(15), Durations.seconds(5))
    resDS.print()
  }

  /**
   * reduceByKeyAndWindow
   * reduceByKeyAndWindow(Function2<Integer, Integer, Integer>(), windowDuration: Duration, slideDuration: Duration)
   *
   * @param ds
   */
  def reduceByKeyAndWindow(ds: DStream[(String, Int)]):Unit = {
    val resDS = ds.reduceByKeyAndWindow((v1:Int, v2:Int) => v1+v2, Durations.seconds(15), Durations.seconds(5))
    resDS.print()
  }

  /**
   * reduceByKeyAndWindow() 的优化版本
   * 其中使用前一窗口的 reduce 值逐渐计算每个窗口的 reduce值。
   * 这是通过减少进入滑动窗口的新数据，以及 “inverse reducing（逆减）” 离开窗口的旧数据来完成的。
   *
   * 一个例子是当窗口滑动时”添加” 和 “减” keys 的数量。
   * 然而，它仅适用于 “invertible reduce functions（可逆减少函数）”，
   * 即具有相应 “inverse reduce（反向减少）” 函数的 reduce 函数（作为参数 invFunc </ i>）。
   * 像在 reduceByKeyAndWindow 中的那样，reduce 任务的数量可以通过可选参数进行配置。
   *
   * 请注意，针对该操作的使用必须启用 checkpointing.
   *
   * @param streamingContext
   * @param ds
   */
  def reduceByKeyAndWindowInv(streamingContext: StreamingContext, ds: DStream[(String, Int)]):Unit = {
    streamingContext.checkpoint("./data/streaming/ck2")
    val resDS = ds.reduceByKeyAndWindow(
      (v1, v2) => v1+v2,
      (v1, v2) => v1+v2,
      Durations.seconds(15),
      Durations.seconds(5)
    )
    resDS.print()
  }
}

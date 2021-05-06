package top.theonly.spark.sca.streaming

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function0
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.streaming.api.java.JavaStreamingContext

/**
 * Driver HA（Standalone或者Mesos）     只适用于业务逻辑一成不变的 SparkStreaming程序
 * 因为SparkStreaming是7*24小时运行，Driver只是一个简单的进程，有可能挂掉，
 * 所以实现Driver的HA就有必要（如果使用的Client模式就无法实现Driver HA ，这里针对的是cluster模式）。
 * Yarn平台cluster模式提交任务，AM(AplicationMaster)相当于Driver，如果挂掉会自动启动AM。
 * 这里所说的DriverHA针对的是Spark standalone和Mesos资源调度的情况下。实现Driver的高可用有两个步骤:
 * 1、提交任务层面，在提交任务的时候加上选项 --supervise,当Driver挂掉的时候会自动重启Driver
 * 2、代码层面，使用JavaStreamingContext.getOrCreate（checkpoint路径，JavaStreamingContextFactory）
 *  Driver中元数据包括：
 *                  1. 创建应用程序的配置信息。
 *                  2. DStream的操作逻辑。
 *                  3. job中没有完成的批次数据，也就是job的执行进度。
 *
 */
object SparkStreamingDriverHA {
  /** checkpoint目录 */
    private val CHECKPOINT_DIR = "./data/streaming/driver_ha_ck"

  @throws[InterruptedException]
  def main(args: Array[String]) = {
    /**
     * getOrCreate()
     * 该方法首先会从CHECKPOINT_DIR目录中获取StreamingContext
     * 若能获取到StreamingContext，就不会执行createStreamingContext()方法来创建，否则执行创建
     */
    val streamingContext = StreamingContext.getOrCreate(CHECKPOINT_DIR, createStreamingContext)
    streamingContext.start()
    streamingContext.awaitTermination()
    streamingContext.stop()
  }

  def createStreamingContext() = { // 创建SparkConf
    val conf = new SparkConf
    // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
    conf.setMaster("local[2]")
    // 设置应用程序名称
    conf.setAppName("SparkStreamingReadSocket")
    // 创建SparkContext
    val sparkContext = new SparkContext(conf)
    // 创建StreamingContext（java中为JavaStreamingContext）
    // 参数1：JavaSparkContext，参数2：batchDuration : org.apache.spark.streaming.Duration，多长时间生成一批次数据
    val streamingContext = new StreamingContext(sparkContext, Durations.seconds(5))
    // 设置checkpoint目录，保存Driver元数据
    /**
     * Driver中元数据包括：
     *   1. 创建应用程序的配置信息。
     *   2. DStream的操作逻辑。
     *   3. job中没有完成的批次数据，也就是job的执行进度。
     */
    streamingContext.checkpoint(CHECKPOINT_DIR)
    // 切割，计数，聚合统计
    val pairMapDS = streamingContext.socketTextStream("node0", 9999)
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey((v1: Int, v2: Int) => v1 + v2)
    // 先前逻辑
    pairMapDS.print()

    /**
     * 当Driver down掉后，重启，新的逻辑是不会执行的
     * 如果想要执行：1、删除checkpoint目录  2、重启时重新指定checkpoint目录
     * 这样的话job的历史执行进度就会没有，因此DriverHA 不适用于会修改业务逻辑的SparkStreaming程序
     */
    // 新的逻辑
    //        pairMapDS.foreachRDD(rdd -> {
    //            JavaPairRDD<String, Integer> filter = rdd.filter(tp -> true);
    //            filter.foreach(tp-> System.out.println(tp));
    //        });
    streamingContext
  }
}

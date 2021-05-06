package top.theonly.spark.sca.streaming.operator

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * updateStateByKey
 *    transformation算子
 *
 * updateStateByKey作用：
 *      1)为SparkStreaming中每一个Key维护一份state状态，state类型可以是任意类型的，可以是一个自定义的对象，
 *        更新函数也可以是自定义的。
 *      2)通过更新函数对该key的状态不断更新，对于每个新的batch而言，
 *        SparkStreaming会在使用updateStateByKey的时候为已经存在的key进行state的状态更新。
 *
 * 使用到updateStateByKey要开启checkpoint机制和功能
 *
 * 多久会将内存中的数据写入到磁盘一份？
 *      如果batchInterval设置的时间小于10秒，那么10秒写入磁盘一份。
 *      如果batchInterval设置的时间大于10秒，那么就会batchInterval时间间隔写入磁盘一份。
 */
object UpdateStateByKeyTest {

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
    val ds2 =
    // 将每行数据按空格切分，每行一个单词，生成新的DStream
      ds.flatMap(line => line.split(" "))
        // 对每行的单词的出现次数计数1
        .map(word => (word, 1))

    /**
     * updateStateByKey
     */
    // 设置checkpoint目录
    streamingContext.checkpoint("./data/streaming/ck2")
    val resDS: DStream[(String, Int)] = ds2.updateStateByKey((seq: Seq[Int], option: Option[Int]) => {
      var preValue: Int = option.getOrElse(0)
      seq.foreach(i => preValue += i)
      Option[Int](preValue)
    })
    resDS.print()

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
}

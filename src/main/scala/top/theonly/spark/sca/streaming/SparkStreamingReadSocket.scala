package top.theonly.spark.sca.streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Durations, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 读取Socket数据，进行wordcount
 * 1、node0服务器开启一个socket服务 nc -lk 9999    yum install -y nc
 * 2、启动，运行main方法
 * 3、在node0的socket服务中发送消息，格式：hello spark
 * 4、检测控制台输出
 *
 * @author theonly
 */
object SparkStreamingReadSocket {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf: SparkConf = new SparkConf()
    // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
    conf.setMaster("local[2]")
    // 设置应用程序名称
    conf.setAppName("SparkStreamingReadSocket")
    // 创建SparkContext
    val context: SparkContext = new SparkContext(conf)
    // 设置日志级别
    context.setLogLevel("ERROR")
    // 创建StreamingContext（java中为JavaStreamingContext）
    // 参数1：JavaSparkContext，参数2：batchDuration : org.apache.spark.streaming.Duration，多长时间生成一批次数据
    val streamingContext: StreamingContext = new StreamingContext(context, Durations.seconds(5))

    // 参数1也可以使用SparkConf对象，但该应用程序中不能出现SparkContext对象
//        val streamingContext: StreamingContext = new StreamingContext(conf, Durations.seconds(5));

    // 从socket服务中读取数据流（指定socket服务的主机名和端口），结果封装为ReceiverInputDStream
    val ds: ReceiverInputDStream[String] = streamingContext.socketTextStream("node0", 9999)
    val resDS: DStream[(String, Int)] =
      // 将每行数据按空格切分，每行一个单词，生成新的DStream
      ds.flatMap(line => line.split(" "))
      // 对每行的单词的出现次数计数1
      .map(word => (word, 1))
      // 对tuple2中key即单词进行分组，将次数累加
      .reduceByKey((v1, v2) => v1 + v2)

    // 控制台打印
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

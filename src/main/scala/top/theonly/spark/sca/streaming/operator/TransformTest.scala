package top.theonly.spark.sca.streaming.operator

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
 * transform：Java中为 transformToPair
 *    可以获取DStream中的RDD，对RDD使用RDD的算子进行操作，要求返回RDD，最后将RDD封装为DStream
 *    transform算子内获取的RDD的算子外的代码是在Driver端执行的。可以动态改变广播变量
 *
 * @author theonly
 */
object TransformTest {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf()
    // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
    conf.setMaster("local[2]")
    // 设置应用程序名称
    conf.setAppName("TransformTest")
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

    /**
     * transform
     */
    val resDS: DStream[(String, Int)] = ds.transform(rdd => {
      rdd.flatMap(line => line.split(" "))
        .map(word => (word, 1))
        .reduceByKey((v1, v2) => v1 + v2)
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

package top.theonly.spark.sca.streaming.operator

import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext.rddToFileName
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext, Time}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

import scala.reflect.ClassTag

/**
 * DStreams 上的输出操作
 *
 * 输出操作允许将 DStream 的数据推送到外部系统，如数据库或文件系统。
 * 由于输出操作实际上允许外部系统使用变换后的数据，所以它们触发所有 DStream 变换的实际执行（类似于RDD的动作）。
 * 目前，定义了以下输出操作：
 * print()
 * foreachRDD(func)
 * // 以下都是存储在Hadoop的hdfs中
 * saveAsTextFiles(prefix, [suffix])
 * saveAsObjectFiles(prefix, [suffix])
 * saveAsHadoopFiles(prefix, [suffix])
 *
 * @author theonly
 */
object SaveTest {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf()
    // 设置运行模式master为local，local[2]表示使用本地机器，使用cpu核心数core为2
    conf.setMaster("local[2]")
    // 设置应用程序名称
    conf.setAppName("SaveTest")
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
    val resDS : DStream[(String, Int)] =
    // 将每行数据按空格切分，每行一个单词，生成新的DStream
      ds.flatMap(line => line.split(" "))
        // 对每行的单词的出现次数计数1
        .map(word => (word, 1))
        // 对tuple2中key即单词进行分组，将次数累加
        .reduceByKey((v1, v2) => v1 + v2)

    /**
     * save
     * 保存到hadoop
     */
    resDS.saveAsTextFiles("wc", "txt")
//    resDS.saveAsObjectFiles("wc", "txt")
//    resDS.saveAsHadoopFiles("wc", "txt", classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[Text, IntWritable]])
//    resDS.saveAsNewAPIHadoopFiles("wc", "txt", classOf[Text], classOf[IntWritable], classOf[TextOutputFormat[Text, IntWritable]])

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
   * saveAsHadoopFile， 保存文件到hadoop，就是saveAsTextFiles的底层实现
   * @param ds
   * @param prefix
   * @param suffix
   */
  def saveAsHadoopFiles(ds: DStream[(String, Int)], prefix: String, suffix: String): Unit = {

    ds.foreachRDD((rdd, time) => {
      val file: String = rddToFileName(prefix, suffix, time)
      val r: RDD[(Text, IntWritable)] = rdd.mapPartitions { iter => {
        val text: Text = new Text()
        val intWritable: IntWritable = new IntWritable()
        iter.map { x =>
          text.set(x._1)
          intWritable.set(x._2)
          (text, intWritable)
        }
      }
      }
      RDD.rddToPairRDDFunctions(r)(implicitly[ClassTag[Text]], implicitly[ClassTag[IntWritable]])
        .saveAsHadoopFile[TextOutputFormat[Text, IntWritable]](file)
    })

  }

  /**
   *
   * @param prefix
   * @param suffix
   * @param time
   * @tparam T
   * @return
   */
  private[streaming] def rddToFileName[T](prefix: String, suffix: String, time: Time): String = {
    var result = time.milliseconds.toString
    if (prefix != null && prefix.length > 0) {
      result = s"$prefix-$result"
    }
    if (suffix != null && suffix.length > 0) {
      result = s"$result.$suffix"
    }
    result
  }
}

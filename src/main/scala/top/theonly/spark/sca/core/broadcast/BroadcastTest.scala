package top.theonly.spark.sca.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 广播变量
 *       SparkContext.broadcast(value: T)
 *
 *    当Executor端使用到了Driver端的变量时，可以使用广播变量来减少Executor端的内存使用
 *
 *    注意问题：
 *      1)、不能将RDD广播出去，可以将RDD收集到的结果回收到Driver端之后再广播
 *      2)、广播变量只能在Driver端定义，在Executor端不能修改广播变量的值
 */
object BroadcastTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SecondSortCase")

    val context: SparkContext = new SparkContext(conf)
    // 变量
    val list: List[String] = List[String]("hello spark")
    // 将变量广播到Executor端
    val bc: Broadcast[List[String]] = context.broadcast(list)

    val rdd: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt")

    /**
     * 使用广播变量
     */
    val filterRDD1: RDD[String] = rdd.filter(line => {
      // 获取广播变量的值
      val value: List[String] = bc.value
      value.contains(line)
    })

    /**
     * 不使用广播变量
     */
    val filterRDD2: RDD[String] = rdd.filter(line => {
      list.contains(line)
    })

    // 结果是一样的
    filterRDD1.foreach(println)
    filterRDD2.foreach(println)
  }

}

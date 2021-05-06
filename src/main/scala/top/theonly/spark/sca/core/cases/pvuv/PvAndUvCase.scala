package top.theonly.spark.sca.core.cases.pvuv

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PvAndUvCase {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("PvAndUvCase")
    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\pvuvdata")

    /**
     * 计算PV
     */
    val pvMap: collection.Map[String, Int] =
      // 网站url每出现一次就记一次数，将网站url作为key，value为1
      rdd.map(line => (line.split("\t")(5), 1))
      // 通过key进行聚合，将次数相加
      .reduceByKey(_ + _)
      // 根据PV数量进行降序排序
      .sortBy(_._2, false)
      // 将结果收集为map
      .collectAsMap()
    pvMap.foreach(tuple2=>println(s"网站：${tuple2._1}，PV量：${tuple2._2}"))

    /**
     * 计算UV：通过userId
     */
    val uvMap: collection.Map[String, Int] =
      // 对每一行进行处理，userId_website
      rdd.map(line => line.split("\t")(4) + "_" + line.split("\t")(5))
      // 去重
      .distinct()
      // 去重后在对website进行计数，出现一次，计数1
      .map(line => (line.split("_")(1), 1))
      // 通过key进行聚合，将次数相加
      .reduceByKey(_ + _)
      // 对UV量降序排序
      .sortBy(_._2, false)
      // 将收集手机为map
      .collectAsMap()
    uvMap.foreach(tuple2 => println(s"网站：${tuple2._1}，UV量：${tuple2._2}"))
  }

}

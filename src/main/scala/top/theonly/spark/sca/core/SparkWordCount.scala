package top.theonly.spark.sca.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object SparkWordCount {

  def main(args: Array[String]): Unit = {

    /**
     * 简化代码
     */
    val conf: SparkConf = new SparkConf().setAppName("scala-wordcount").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val path = "C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt"
    context.textFile(path).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).foreach(print)


    /**
     * 完整代码
     */
//    val conf: SparkConf = new SparkConf()
//    conf.setAppName("scala-wordcount")
//    conf.setMaster("local")
//    val context: SparkContext = new SparkContext(conf)
//    val lines: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt")
//    val words: RDD[String] = lines.flatMap(str=>str.split(" "))
//
//    val partWord: RDD[(String, Int)] = words.map(str => Tuple2(str, 1))
//    /*
//    reduceByKey：先按照key进行分组，在对每个组内key的value值进行聚合
//     */
//    val res: RDD[(String, Int)] = partWord.reduceByKey((v1: Int, v2: Int) => v1+v2)
//    res.foreach(tup=> {
//      println(s"key: ${tup._1}, count: ${tup._2}")
//    })
  }
}

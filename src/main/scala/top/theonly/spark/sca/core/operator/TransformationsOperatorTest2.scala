package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * 补充Transformation算子
 *
 *  join,leftOuterJoin,rightOuterJoin,fullOuterJoin
 *    作用在K,V格式的RDD上。根据K进行连接，对（K,V）join(K,W)返回（K,(V,W)）
 *     join后的分区数与父RDD分区数多的那一个相同。
 */
object TransformationsOperatorTest2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Transformation-test")
    val context = new SparkContext(conf)

    val ageRDD: RDD[(String, Int)] = context.parallelize(Array[Tuple2[String, Int]](
      ("zhangsan", 19), ("lisi", 20), ("wangwu", 21), ("zhaoliu", 22)))
    val scoreRDD: RDD[(String, Int)] = context.parallelize(Array[Tuple2[String, Int]](
      ("zhangsan", 96), ("lisi", 77), ("wangwu", 85), ("maqi", 89)))

    /**
     * join：
     *    只对相同key进行join，不同key舍去
     */
//    val joinRDD: RDD[(String, (Int, Int))] = ageRDD.join(scoreRDD)
//    joinRDD.foreach(println)

    /**
     * leftOuterJoin：
     *    以左边的RDD的key为准
     */
//    val leftJoinRDD: RDD[(String, (Int, Option[Int]))] = ageRDD.leftOuterJoin(scoreRDD)
//    leftJoinRDD.foreach(tp => {
//        println(s"name=${tp._1}, age=${tp._2._1}, score=${tp._2._2.getOrElse(null)}")
////        println(s"name=${tp._1}, age=${tp._2._1}, score=${tp._2._2}")
//    })


    /**
     * rightOuterJoin：
     *    以右边的RDD的key为准
     */
//    val rightJoinRDD: RDD[(String, (Option[Int], Int))] = ageRDD.rightOuterJoin(scoreRDD)
//
//    rightJoinRDD.foreach(tp => {
//        println(s"name=${tp._1}, age=${tp._2._1.getOrElse("None")}, score=${tp._2._2}")
//    })

    /**
     * fullOuterJoin：
     *    两边的RDD的key都会包含
     */
    val fullJoinRDD: RDD[(String, (Option[Int], Option[Int]))] = ageRDD.fullOuterJoin(scoreRDD)

    fullJoinRDD.foreach(tp => {
      println(s"name=${tp._1}, age=${tp._2._1.getOrElse("None")}, score=${tp._2._2.getOrElse("None")}")
    })

  }
}

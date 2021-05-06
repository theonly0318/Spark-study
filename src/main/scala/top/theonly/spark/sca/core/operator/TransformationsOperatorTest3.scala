package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 *
 *  union
 *    合并两个数据集。两个数据集的类型要一致。
 *
 *     返回新的RDD的分区数是合并RDD分区数的总和。
 *
 *  intersection
 *    取两个数据集的交集，返回新的RDD与父RDD分区多的一致
 *
 *  subtract
 *    取两个数据集的差集，结果RDD的分区数与subtract前面的RDD的分区数一致。
 *
 *
 *  mapPartitions
 *    与map类似，遍历的单位是每个partition上的数据。
 *
 *  distinct(map+reduceByKey+map)
 *
 *  cogroup
 *    当调用类型（K,V）和（K，W）的数据上时，返回一个数据集（K，（Iterable<V>,Iterable<W>）），子RDD的分区与父RDD多的一致。
 */
object TransformationsOperatorTest3 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationsOperatorTest3")

    val context: SparkContext = new SparkContext(conf)

    val rdd1: RDD[String] = context.parallelize(Array[String]("a", "b", "c", "d"))
    val rdd2: RDD[String] = context.parallelize(Array[String]("c", "d", "e", "f"))

    /**
     * union
     */
//    val unionRDD: RDD[String] = rdd1.union(rdd2)
//    unionRDD.foreach(println)

    /**
     * intersection：取交集
     */
//    val interRDD: RDD[String] = rdd1.intersection(rdd2)
//    interRDD.foreach(println)

    /**
     * subtract：取差集
     */
//    val subRDD: RDD[String] = rdd1.subtract(rdd2)
//    subRDD.foreach(println)
//
//    val subRDD2: RDD[String] = rdd2.subtract(rdd1)
//    subRDD2.foreach(println)


    val rdd3: RDD[String] = context.parallelize(Array[String]("a", "b", "c", "d"), 2)

    /**
     * mapPartitions：遍历每个partition上的数据
     */
//    val res: RDD[String] = rdd3.mapPartitions(iter => {
//      val listBuffer = new ListBuffer[String]
//      while (iter.hasNext) {
//        val elem = iter.next()
//        listBuffer.append(elem)
//      }
//      println("插入数据库:" + listBuffer.toString())
//      println("关闭数据库")
//      listBuffer.iterator
//    })
//    res.count()


    /**
     * distinct：去重
     */
    val rdd4: RDD[String] = context.parallelize(Array[String]("a", "b", "c", "b"), 2)
//    val disRDD: RDD[String] = rdd4.distinct()
//    disRDD.foreach(println)

    /**
     * cogroup
     */
    val rdd5: RDD[(String, Int)] = context.parallelize(Array[(String, Int)](("a", 100), ("b", 200), ("c", 300), ("b", 400)))
    val rdd6: RDD[(String, Int)] = context.parallelize(Array[(String, Int)](("a", 111), ("b", 222), ("c", 333), ("b", 444)))
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd5.cogroup(rdd6)
    cogroupRDD.foreach(println)

  }

}

package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * transformations 类算子
 *
 *  mapPartitionWithIndex
 *    类似于mapPartitions,除此之外还会携带分区的索引值。
 *
 *  repartition
 *    增加或减少分区。会产生shuffle。（多个分区分到一个分区不会产生shuffle）
 *
 *  coalesce
 *    coalesce常用来减少分区，第二个参数是减少分区的过程中是否产生shuffle。
 *    true为产生shuffle，false不产生shuffle。默认是false。
 *    如果coalesce设置的分区数比原来的RDD的分区数还多的话，第二个参数设置为false不会起作用，如果设置成true，效果和repartition一样。
 *    即repartition(numPartitions) = coalesce(numPartitions,true)
 */
object TransformationsOperatorTest4 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationsOperatorTest4")
    val context = new SparkContext(conf)
    val rdd: RDD[String] = context.parallelize(Array[String](
      "hello1", "hello2", "hello3", "hello4",
      "hello5", "hello6", "hello7", "hello8",
      "hello9", "hello10", "hello11", "hello12"), 3)



    /**
     * mapPartitionsWithIndex(Function2<Integer, Iterator<String>, Iterator<String>>(), preservesPartitioning)
     *    preservesPartitioning：是否保留原RDD的分区策略（哈希）
     */
    val mpRDD1 = rdd.mapPartitionsWithIndex((index, iterator) => {
      val listBuffer = new ListBuffer[String]
      while (iterator.hasNext) {
        val next = iterator.next()
        listBuffer.append(s"rdd partition num is $index, value is $next")
      }
      listBuffer.iterator
    }, true)
    val collect: Array[String] = mpRDD1.collect()
    for (elem <- collect) {println(elem)}



    /**
     * repartition(numPartitions: Int)
     */
    val repartitionRDD = mpRDD1.repartition(4)
    val mpRDD2 = repartitionRDD.mapPartitionsWithIndex((index, iterator) => {
      val listBuffer = new ListBuffer[String]
      while (iterator.hasNext) {
        val next = iterator.next()
        listBuffer.append(s"rdd partition num is $index, value is $next")
      }
      listBuffer.iterator
    }, true)
    val collect1: Array[String] = mpRDD2.collect()
    for (elem <- collect1) {println(elem)}



    /**
     * coalesce(numPartitions: Int, shuffle: Boolean)
     *      shuffle：是否产生shuffle，默认为false
     *
     *  coalesce(numPartitions,true) = repartition(numPartitions)
     */
//    val coalesceRDD: RDD[String] = mpRDD1.coalesce(4, true)
//    val coalesceRDD: RDD[String] = mpRDD1.coalesce(2, true)
    val coalesceRDD: RDD[String] = mpRDD1.coalesce(4, false)
    val mpRDD3 = coalesceRDD.mapPartitionsWithIndex((index, iterator) => {
      val listBuffer = new ListBuffer[String]
      while (iterator.hasNext) {
        val next = iterator.next()
        listBuffer.append(s"rdd partition num is $index, value is $next")
      }
      listBuffer.iterator
    }, true)
    val collect2: Array[String] = mpRDD2.collect()
    for (elem <- collect2) {println(elem)}

  }

}

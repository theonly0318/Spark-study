package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * transformations 类算子
 *
 *  groupByKey
 *    作用在K，V格式的RDD上。根据Key进行分组。作用在（K，V），返回（K，Iterable <V>）。
 *
 *  zip
 *    将两个RDD中的元素（KV格式/非KV格式）变成一个KV格式的RDD,两个RDD的每个分区元素个数必须相同。
 *
 *  zipWithIndex
 *    该函数将RDD中的元素和这个元素在RDD中的索引号（从0开始）组合成（K,V）对。
 *
 *  mapValues
 *    针对K,V格式的RDD,该函数对K,V格式RDD中的value做操作，返回是K,V格式的RDD.
 */
object TransformationsOperatorTest5 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationsOperatorTest4")
    val context = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = context.parallelize(List(
      new Tuple2("a", 1),
      new Tuple2("b", 3),
      new Tuple2("a", 6),
      new Tuple2("c", 2),
      new Tuple2("b", 9)))

    /**
     * groupByKey
     *
     * (a,CompactBuffer(1, 6))
     * (b,CompactBuffer(3, 9))
     * (c,CompactBuffer(2))
     */
    val gbkRDD = rdd1.groupByKey()
    gbkRDD.foreach(tp=>println(tp))

    /**
     * mapValues
     *
     * (a,2)
     * (b,6)
     * (a,12)
     * (c,4)
     * (b,18)
     */
    val mvRDD: RDD[(String, Int)] = rdd1.mapValues(v => v * 2)
    mvRDD.foreach(tp=>println(tp))


    val rdd2: RDD[String] = context.parallelize(List("a", "b", "c", "d"))
    val rdd3: RDD[Int] = context.parallelize(List(1, 2, 3, 4))
    /**
     * zip
     *
     * (a,1)
     * (b,2)
     * (c,3)
     * (d,4)
     */
    val zipRDD: RDD[(String, Int)] = rdd2.zip(rdd3)
    zipRDD.foreach(println)

    /**
     * zipWithIndex
     *
     * (a,0)
     * (b,1)
     * (c,2)
     * (d,3)
     */
    val zwiRDD: RDD[(String, Long)] = rdd2.zipWithIndex()
    zwiRDD.foreach(println)
  }

}

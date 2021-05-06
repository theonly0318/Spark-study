package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Action 触发算子
 *
 *  countByKey
 * 作用到K,V格式的RDD上，根据Key计数相同Key的数据集元素。
 *
 *  countByValue
 * 根据数据集每个元素相同的内容来计数。返回相同内容的元素对应的条数。
 */
object ActionOperatorTest3 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setAppName("scala-wordcount")
    conf.setMaster("local")
    val context = new SparkContext(conf)
    val rdd1: RDD[(String, Int)] = context.parallelize(List(
      new Tuple2("a", 1),
      new Tuple2("b", 2),
      new Tuple2("b", 5),
      new Tuple2("c", 3),
      new Tuple2("c", 3),
      new Tuple2("d", 4)))

    /**
     * countByKey
     * (d,1)
     * (a,1)
     * (b,2)
     * (c,2)
     */
    val map1: collection.Map[String, Long] = rdd1.countByKey()
    for (elem <- map1) {println(elem)}

    /**
     * countByValue
     * ((c,3),2)
     * ((b,2),1)
     * ((b,5),1)
     * ((d,4),1)
     * ((a,1),1)
     */
    val map2: collection.Map[(String, Int), Long] = rdd1.countByValue()
    for (elem <- map2) {println(elem)}
  }
}

package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Action 触发算子
 *
 *  foreachPartition
 *    遍历的数据是每个partition的数据。
 *
 *  collectAsMap
 *    对K,V格式的RDD数据回收转换成Map<K,V>
 *
 *  takeSample(boolean,num，seed)
 *    takeSample可以对RDD中的数据随机获取num个，第一个参数是有无放回，第二个参数是随机获取几个元素，第三个参数如果固定，那么每次获取的数据固定。
 *
 *  top(num)
 *    对RDD中的所有元素进行由大到小排序，获取前num个元素返回。
 *
 *  takeOrdered(num)
 *    对RDD中的所有元素进行由小到大的排序，获取前num个元素返回。
 */
object ActionOperatorTest2 {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
    conf.setAppName("scala-wordcount")
    conf.setMaster("local")
    val context = new SparkContext(conf)
    val rdd: RDD[String] = context.parallelize(List(
      "hello1", "hello2", "hello3", "hello4",
      "hello5", "hello6", "hello7", "hello8",
      "hello9", "hello10", "hello11", "hello12"), 3)

    /**
     * foreachPartition
     */
    rdd.foreachPartition(iterable => {
      while (iterable.hasNext) {
        println(iterable.next())
      }
    })

    /**
     * 抽样
     * takeSample(withReplacement: Boolean, num: Int, seed: Long)
     *    - withReplacement：是否替换（有无放回），true：有放回，可能抽到相同数据
     *    - num：抽取元素个数
     *    - seed：种子，相同种子，多次抽取的数据是一致的
     */
    val arr1 = rdd.takeSample(true, 5, 100)
    for (elem <- arr1) {println(elem)}


    /**
     * 对RDD中的所有元素进行由大到小排序，获取前num个元素返回。
     * top(num: Int, comp: Comparator[T])
     * 可以自定义比较器
     */
    val arr2 = rdd.top(5)
    for (elem <- arr2) {println(elem)}

    /**
     * takeOrdered(num: Int, comp: Comparator[T])
     * 对RDD中的所有元素进行由小到大的排序，获取前num个元素返回。
     * 可以自定义比较器
     */
    val arr3 = rdd.takeOrdered(5)
    for (elem <- arr3) {println(elem)}


    val rdd2: RDD[(String, Int)] = context.parallelize(List(
      new Tuple2("张三", 22),
      new Tuple2("李四", 23),
      new Tuple2("王五", 24),
      new Tuple2("赵六", 25)))

    /**
     * collectAsMap
     */
    val map = rdd2.collectAsMap()
    for (elem <- map) {println(elem)}
  }
}

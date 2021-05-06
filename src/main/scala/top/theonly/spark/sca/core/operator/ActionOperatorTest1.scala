package top.theonly.spark.sca.core.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 *
 * Action 触发算子
 *     Action类算子也是一类算子（函数）叫做行动算子，如foreach,collect，count等
 *     触发Transformations类算子执行
 *     Transformations类算子是延迟执行，Action类算子是触发执行。
 *     一个application应用程序中有几个Action类算子执行，就有几个job运行。
 *
 *
 * count
 * 返回数据集中的元素数。会在结果计算完成后回收到Driver端。
 *
 * take(n)
 * 返回一个包含数据集前n个元素的集合。
 *
 * first
 * first=take(1),返回数据集中的第一个元素。
 *
 * foreach
 * 循环遍历数据集中的每个元素，运行相应的逻辑。
 *
 * collect
 * 将计算结果回收到Driver端。
 */
object ActionOperatorTest1 {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf()
    conf.setAppName("scala-wordcount")
    conf.setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt")

    /**
     * 转换算子 flatMap
     */
    val words: RDD[String] = lines.flatMap(str=>str.split(" "))

    /**
     * 转换算子 map
     */
    val partWord: RDD[(String, Int)] = words.map(str => Tuple2(str, 1))

    /*
    reduceByKey：先按照key进行分组，在对每个组内key的value值进行聚合
     */
    /**
     * 转换算子 reduceByKey
     */
    val ret: RDD[(String, Int)] = partWord.reduceByKey((v1: Int, v2: Int) => v1+v2)

    /**
     * 行动（触发）算子foreach
     */
    ret.foreach(println)

    /**
     * 行动（触发）算子count()
     */
//    val count: Long = ret.count()
//    println(count)

    /**
     * 行动（触发）算子take(num: Int)
     */
//    val tuples: Array[(String, Int)] = ret.take(2)
//    for (elem <- tuples) {
//      println(elem)
//    }

    /**
     * 行动（触发）算子first()  等同于take(1)
     */
//    val tuple: (String, Int) = ret.first()
//    println(tuple)

    /**
     * 行动（触发）算子collect()
     */
//    val tuples: Array[(String, Int)] = ret.collect()
//    for (elem <- tuples) println(elem)

  }
}

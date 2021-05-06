package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Transformation 转换算子
 *     Transformations类算子是一类算子（函数）叫做转换算子
 *     如map,flatMap,reduceByKey等。
 *     Transformations算子是延迟执行，也叫懒加载执行。必须有触发算子触发才会执行
 *
 * filter
 *     过滤符合条件的记录数，true保留，false过滤掉。
 *
 * map
 *     将一个RDD中的每个数据项，通过map中的函数映射变为一个新的元素。
 *     特点：输入一条，输出一条数据。
 *
 * flatMap
 *     先map后flat。与map类似，每个输入项可以映射为0到多个输出项。
 *
 * sample
 *     随机抽样算子，根据传进去的小数按比例进行又放回或者无放回的抽样。
 *
 * reduceByKey
 *     将相同的Key根据相应的逻辑进行处理。
 *
 * sortByKey/sortBy
 *     作用在K,V格式的RDD上，对key进行升序或者降序排序。
 */
object TransformationsOperatorTest1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("scala-wordcount")
    conf.setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt")

    /**
     * 转换算子 sample
     * 抽样
     * 参数：
     *  withReplacement：有无放回抽样；true：有放回（可能会抽到同一条数据），false：无放回
     *  fraction：比例因子，小于1的Double类型；是不精确的
     *  seed：种子，Long类型；给定相同种子后，数据每次抽样得到的结果不变
     */
//    val res: RDD[(String, Int)] = lines.sample(true, 0.5)

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
     * 转换算子 sortBy
     * 排序
     */
    // 根据tuple第二个值倒序排列
//    val res: RDD[(String, Int)] = ret.sortBy(tup => tup._2, false)

    /**
     * 转换算子 sortByKey
     * 排序
     */

//    val res: RDD[(String, Int)] = ret.sortByKey(true)

    /**
     * 转换算子 filter
     * 过滤
     */
    val res: RDD[(String, Int)] = ret.filter(tup => tup._2 > 20)

    res.foreach(println)
  }
}

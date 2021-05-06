package top.theonly.spark.sca.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

/**
 * 累加器
 *      SparkContext.longAccumulator(name:String)
 *    累加器是分布式中的统筹变量，Spark分布式计算过程如果有计数需求，要使用累加器
 *
 *    注意事项：
 *      累加器只能在Driver端定义，在Executor端使用、更新
 *
 */
object AccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SecondSortCase")

    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\topn.txt")

    // 创建累加器
    val iAcc: LongAccumulator = context.longAccumulator("i")

    val mapRDD1 = rdd.map(line => {
      // 累加器计数+1
      iAcc.add(1)
      line
    })

    mapRDD1.count()
    // acc i is 24
    println(s"acc ${iAcc.name.get} is ${iAcc.count}")
    // 24
    println(iAcc.value)


//    var i:Int = 0;
//    val mapRDD2 = rdd.map(line => {
//      i = i+1
//      println(s"Executor i is $i")
//      line
//    })
//
//    mapRDD2.count()
//    // i = 0
//    println(s"Driver i is $i")

  }
}

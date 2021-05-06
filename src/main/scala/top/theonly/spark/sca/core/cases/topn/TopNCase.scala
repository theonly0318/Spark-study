package top.theonly.spark.sca.core.cases.topn

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks

/**
 * 分组 topN
 */
object TopNCase {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SecondSortCase")

    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\topn.txt")

    val mapRDD: RDD[(String, Int)] = rdd.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    /**
     * 原生集合方式
     */
//    val topRDD: RDD[(String, List[Int])] = groupRDD.map(tp => {
////      val scoreArr: Iterable[Int] = tp._2
////      val list: List[Int] = scoreArr.toList
////      val sorted: List[Int] = list.sortWith((v1, v2) => v1>v2)
////      val ints: List[Int] = sorted.take(3)
//      val ints: List[Int] = tp._2.toList.sortWith((v1, v2) => v1>v2).take(3)
//      new Tuple2(tp._1, ints)
//    })
//    topRDD.foreach(tp => println(tp))

    /**
     * 定长数组方式
     */
    val topRDD: RDD[(String, Array[Int])] = groupRDD.map(tp => {
      val iterator: Iterator[Int] = tp._2.iterator
      val array: Array[Int] = new Array[Int](3)
      val breaks: Breaks = new Breaks
      while (iterator.hasNext) {
        val next: Int = iterator.next()

        breaks.breakable{
          for (i <- 0 until(array.length)) {
            if (array(i) == 0) {
              array(i) = next
              breaks.break()
            } else if(next > array(i)) {
              for (j <- 2 until(i, -1)) {
                array(j) = array(j-1)
              }
              array(i) = next
              breaks.break()
            }
          }
        }
      }

      (tp._1, array)
    })

    topRDD.foreach(tp => {
      println(tp._1)
      tp._2.foreach(println)
    })
  }

}

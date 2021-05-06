package top.theonly.spark.sca.core.cases.secondsort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 二次排序
 * 对secondsort.txt文件中的每一行进行排序（降序），先对第一个数字排序，再对第二个数字排序
 */
object SecondSortCase {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SecondSortCase")

    val context: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\secondsort.txt")

    val mapRDD: RDD[(MySort, String)] = rdd.map(line => {
      val firstNum: Int = line.split(" ")(0).toInt
      val secondNum: Int = line.split(" ")(1).toInt
      Tuple2(new MySort(firstNum, secondNum), line)
    })
    val sortRDD: RDD[(MySort, String)] = mapRDD.sortByKey(false)

    sortRDD.foreach(tp => println(tp._2))


  }

}

/**
 * 自定义tuple2 key
 * @param firstNum
 * @param secondNum
 */
case class MySort(firstNum: Int, secondNum: Int) extends Ordered[MySort] {
  override def compare(that: MySort): Int = {
    if (this.firstNum == that.firstNum) {
      this.secondNum - that.secondNum
    } else {
      this.firstNum - that.firstNum
    }
  }
}

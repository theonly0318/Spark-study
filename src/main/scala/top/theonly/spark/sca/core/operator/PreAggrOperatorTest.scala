package top.theonly.spark.sca.core.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object PreAggrOperatorTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("PreAggrOperatorTest")

    val sparkContext = new SparkContext(conf)

    val rdd = sparkContext.parallelize(Array(
      ("a", 1),
      ("b", 1),
      ("c", 1),
      ("a", 1),
      ("b", 1),
      ("c", 1),
      ("a", 1),
      ("b", 1),
      ("c", 1),
      ("a", 1),
      ("b", 1),
      ("c", 1)
    ), 3)

    /**
     * partition num is 0, value is (a,1)
     * partition num is 0, value is (b,1)
     * partition num is 0, value is (c,1)
     * partition num is 0, value is (a,1)
     * partition num is 1, value is (b,1)
     * partition num is 1, value is (c,1)
     * partition num is 1, value is (a,1)
     * partition num is 1, value is (b,1)
     * partition num is 2, value is (c,1)
     * partition num is 2, value is (a,1)
     * partition num is 2, value is (b,1)
     * partition num is 2, value is (c,1)
     */
    rdd.mapPartitionsWithIndex((index, iter) => {
      while (iter.hasNext) {
        val next = iter.next()
        println(s"partition is $index, value is $next")
      }
      iter
    }, true).count()

    /**
     * aggregateByKey
     *
     * partition-0
     * (a,1)              (a,2)         (a,2+1+1)     (a,4)
     * (b,1)         =>   (b,2)     =>  (b,2+1)       (b,3)
     * (c,1)              (c,2)         (c,2+1)       (c,3)
     * (a,1)
     * partition-1
     * (b,1)              (a,2)         (a,2+1)       (a,3)         reduce        (a,4+3+3)       (a,10)
     * (c,1)         =>   (b,2)     =>  (b,2+1+1)     (b,4)       ===========>    (b,3+4+3)       (b,10)
     * (a,1)              (c,2)         (c,2+1)       (c,3)                       (c,3+3+4)       (c,10)
     * (b,1)
     * partition-2
     * (c,1)              (a,2)         (a,2+1)       (a,3)
     * (a,1)         =>   (b,2)     =>  (b,2+1)       (b,3)
     * (b,1)              (c,2)         (c,2+1+1)     (c,4)
     * (c,1)
     */
    val rdd3 = rdd.aggregateByKey(2)(
      // 作用在partition
      (v1: Int, v2: Int) => v1 + v2,
      // 作用在reduce端
      (v3: Int, v4: Int) => v3 + v4)

    val rdd4: RDD[(String, Int)] = rdd.combineByKey((v:Int) => v, (v1:Int, v2:Int) => v1+v2, (v3:Int, v4:Int) => v3+v4)

    rdd3.foreach(println)

  }

}

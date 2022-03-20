package top.theonly.spark.sca.core.partition

import org.apache.spark.{SparkConf, SparkContext}

object SelfDefPartitionerTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("SelfDefPartitonerTest").setMaster("local")
    val context: SparkContext = new SparkContext(conf)
  }
}

package top.theonly.spark.sca.core.partition

import org.apache.spark.Partitioner

/**
 * 自定义Spark分区其器
 */
object MyPartitioner extends Partitioner {
  override def numPartitions: Int = 6

  override def getPartition(key: Any): Int = {
    val k: Int = key.toString.toInt
    k % Integer.MAX_VALUE
  }
}

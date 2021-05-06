package top.theonly.spark.sca.core.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * 持久化算子，也叫控制算子
 *    控制算子有三种，cache,persist,checkpoint，以上算子都可以将RDD持久化，持久化的单位是partition。
 *    cache和persist都是懒执行的。必须有一个action类算子触发执行。
 *    checkpoint算子不仅能将RDD持久化到磁盘，还能切断RDD之间的依赖关系。
 *
 *   cache：
 *      默认将RDD的数据持久化到内存中。cache是懒执行。
 *     注意：cache () = persist()=persist(StorageLevel.Memory_Only)
 *   persist：
 *      可以指定持久化的级别。最常用的是 MEMORY_ONLY 和 MEMORY_AND_DISK。"_SER"表示序列化，"_2"表示副本数量。
 *      常用级别：MEMORY_ONLY系列 MEMORY_AND_DISK系列
 *      尽量避免使用DISK_ONLY 相关级别
 *   checkpoint：
 *      checkpoint将RDD持久化到磁盘，还可以切断RDD之间的依赖关系。
 *      checkpoint目录数据当application执行完之后不会被清除。
 *
 *       checkpoint 的执行原理：
 *         1、当RDD的job执行完毕后，会从finalRDD从后往前回溯。
 *         2、当回溯到某一个RDD调用了checkpoint方法，会对当前的RDD做一个标记。
 *         3、Spark框架会自动启动一个新的job，重新计算这个RDD的数据，将数据持久化到HDFS上。
 *
 *       persist(StorageLevel.DISK_ONLY)与Checkpoint的区别？
 *         1、checkpoint需要指定额外的目录存储数据，checkpoint数据是由外部的存储系统管理，不是Spark框架管理，
 *            当application完成之后，不会被清空。
 *         2、cache() 和persist() 持久化的数据是由Spark框架管理，
 *            当application完成之后，会被清空。
 *         3、checkpoint多用于保存状态。
 *
 *       优化：
 *          对RDD执行checkpoint之前，最好对这个RDD先执行cache，
 *          这样新启动的job只需要将内存中的数据拷贝到HDFS上就可以，省去了重新计算这一步。
 *
 * cache和persist的注意事项
 *  1、cache和persist都是懒执行，必须有一个action类算子触发执行。
 *  2、cache和persist算子的返回值可以赋值给一个变量，在其他job中直接使用这个变量就是使用持久化的数据了。持久化的单位是partition。
 *  3、cache和persist算子后不能立即紧跟action算子。
 *  4、cache和persist算子持久化的数据当application执行完成之后会被清除。
 *  错误：rdd.cache().count() 返回的不是持久化的RDD，而是一个数值了。
 *
 *  清除缓存：unpersist(blocking: Boolean = true)
 *    blocking：是否阻塞删除，成功删除所有块后，再删除
 */
object PersistenceOperator {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("scala-wordcount")
    conf.setMaster("local")
    val context: SparkContext = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\word.txt")

    val filterLines: RDD[String] = lines.filter(line => line.contains("spark"))

    /**
     * cache()
     * 可以赋值给新的变量，也可以不复制
     */
    val linesCache: RDD[String] = filterLines.cache()

    /**
     * 清除缓存：unpersist()
     */
//    linesCache.unpersist()

    /**
     * persist(newLevel : org.apache.spark.storage.StorageLevel)
     */
//    filterLines.persist(StorageLevel.MEMORY_ONLY)

    /**
     * checkpoint()
     */
//    // 设置保存磁盘目录
//    context.setCheckpointDir("C:\\code\\IdeaWorkspace\\spark-code\\data\\ck\\")
//    filterLines.checkpoint()
    val count = linesCache.count()

    println(count)
  }
}

package top.theonly.spark.sca.core.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object SelfDefinedAccumulatorTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("SelfDefinedAccumulatorTest")
    val context: SparkContext = new SparkContext(conf)

    // 创建自定义累加器
    val acc: MyAcc = new MyAcc
    // 注册
    context.register(acc, "myAcc")

    // 模拟数据
    val rdd: RDD[String] = context.parallelize(Array[String]("zhangsan,20,0", "lisi,21,1", "wangwu,22,0", "zhaoliu,21,1",
      "tianqi,22,0", "huangba,23,1", "majiu,24,0", "liushi,25,1"), 3)

    // map
    val mapRDD: RDD[String] = rdd.map(line => {
      val split: Array[String] = line.split(",")
      val age: Int = split(1).toInt
      val gender: Int = split(2).toInt
      if (gender == 0) {
        // 累加器相加
        acc.add(PersonInfo(1, age, 0, 1))
      } else if(gender == 1) {
        // 累加器相加
        acc.add(PersonInfo(1, age, 1, 0))
      } else {
        // 累加器相加
        acc.add(PersonInfo(1, age, 0, 0))
      }
      line
    })
    // action算子，触发执行
    mapRDD.count()
    // 获取累加器的值
    // acc myAcc, personCount is 8, ageCount is 178, maleCount is 4, femaleCount is 4
    println(s"acc ${acc.name.getOrElse("null")}, personCount is ${acc.value.personCount}, ageCount is ${acc.value.ageCount}, maleCount is ${acc.value.maleCount}, femaleCount is ${acc.value.femaleCount} ")
  }
}

/**
 * 自定义累加器
 */
class MyAcc extends AccumulatorV2[PersonInfo, PersonInfo] {

  var personInfo = PersonInfo(0, 0,0, 0)

  /**
   * 要与reset方法中的各个参数的初始值比较
   * @return
   */
  override def isZero: Boolean =
    this.personInfo.personCount == 0 && this.personInfo.ageCount == 0 && this.personInfo.maleCount == 0 &&this.personInfo.femaleCount == 0

  /**
   * 从Driver端将自定义的累加器复制到Worker端
   * @return
   */
  override def copy(): AccumulatorV2[PersonInfo, PersonInfo] = {
    val acc: MyAcc = new MyAcc
    acc.personInfo = this.personInfo
    acc
  }

  /**
   * 累加器的重置（初始值），作用在Executor端
   */
  override def reset(): Unit =
    this.personInfo = PersonInfo(0,0,0,0)

  /**
   * 累加器值的相加，作用在Executor端
   * @param v
   */
  override def add(v: PersonInfo): Unit = {
    this.personInfo.personCount += v.personCount
    this.personInfo.ageCount += v.ageCount
    this.personInfo.maleCount += v.maleCount
    this.personInfo.femaleCount += v.femaleCount
  }

  /**
   * 各个分区的累加器的值的合并
   * @param other
   */
  override def merge(other: AccumulatorV2[PersonInfo, PersonInfo]): Unit = {
//    val v: PersonInfo = other.value
    val v: PersonInfo = other.asInstanceOf[MyAcc].personInfo
    this.personInfo.personCount += v.personCount
    this.personInfo.ageCount += v.ageCount
    this.personInfo.maleCount += v.maleCount
    this.personInfo.femaleCount += v.femaleCount
  }

  /**
   * 获取累加器的值
   * @return
   */
  override def value: PersonInfo = this.personInfo
}

/**
 *
 * @param personCount
 * @param ageCount
 * @param maleCount
 * @param femaleCount
 */
case class PersonInfo(var personCount: Int, var ageCount: Int, var maleCount: Int, var femaleCount: Int)

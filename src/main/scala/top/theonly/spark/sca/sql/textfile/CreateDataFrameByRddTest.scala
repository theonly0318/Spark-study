package top.theonly.spark.sca.sql.textfile

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * 通过RDD创建DataFrame
 *  1、json格式的RDD创建DataFrame
 *  2、非json格式的RDD创建DataFrame
 *      1). 通过反射的方式将非json格式的RDD转换成DataFrame（不建议使用）
 *           自定义类要可序列化
 *           自定义类的访问级别是Public
 *           RDD转成DataFrame后会根据映射将字段按Assci码排序
 *           将DataFrame转换成RDD时获取字段两种方式,
 *              一种是df.getInt(0)下标获取（不推荐使用），
 *              另一种是df.getAs(“列名”)获取（推荐使用）
 *
 *      2). 动态创建Schema将非json格式的RDD转换成DataFrame
 */
object CreateDataFrameByRddTest {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local").appName("CreateDataFrameByRddTest").getOrCreate()
    /**
     * 1、通过json格式的RDD创建DataFrame
     */
    createDataFrameByJsonRDD(session)

    /**
     * 2、非json格式的RDD创建DataFrame
     * 通过反射的方式将非json格式的RDD转换成DataFrame（不建议使用）
     */
//    createDataFrameByRDDUseReflect(session)

    /**
     * 2、非json格式的RDD创建DataFrame
     * 动态创建Schema将非json格式的RDD转换成DataFrame
     */
//    createDataFrameByRDDByDynamicSchema(session)
  }

  /**
   * 通过json格式的RDD创建DataFrame
   * @param session
   */
  def createDataFrameByJsonRDD(session: SparkSession): Unit = {
    val jsonList: List[String] = List[String](
      "{'name':'zhangsan','age':'18'}",
      "{'name':'lisi','age':'19'}",
      "{'name':'wangwu','age':'20'}",
      "{'name':'maliu','age':'21'}",
      "{'name':'tainqi','age':'22'}"
    )
    import session.implicits._
    val ds: Dataset[String] = jsonList.toDS()
    val frame1: DataFrame = session.read.json(ds)
    frame1.show()

    /**
     * Spark 1.6
     */
    //    val jsRDD: RDD[String] = session.sparkContext.parallelize(jsonList)
    //    val frame2: DataFrame = session.read.json(jsRDD)
    //    frame2.show()
  }

  /**
   * 非json格式的RDD创建DataFrame
   *    通过反射的方式将非json格式的RDD转换成DataFrame（不建议使用）
   *
   * @param session
   */
  def createDataFrameByRDDUseReflect(session: SparkSession): Unit = {
    val peopleInfo: RDD[String] = session.sparkContext.textFile("./data/sql/people.txt")
    val personRDD: RDD[MyPerson] = peopleInfo.map(info => {
      val split: Array[String] = info.split(",")
      val id: Int = split(0).toInt
      val name: String = split(1).toString
      val age: Int = split(2).toInt
      val score: Int = split(3).toInt
      MyPerson(id, name, age, score)
    })

    import session.implicits._
    val dsp: Dataset[MyPerson] = personRDD.toDS()
    dsp.createTempView("tmp_person")
    session.sql("select * from tmp_person").show()
  }

  /**
   * 非json格式的RDD创建DataFrame
   *  动态创建Schema将非json格式的RDD转换成DataFrame
   * @param session
   */
  def createDataFrameByRDDByDynamicSchema(session: SparkSession): Unit = {
    val peopleInfo2: RDD[String] = session.sparkContext.textFile("./data/people.txt")

    val rowRDD: RDD[Row] = peopleInfo2.map(info => {
      val id = info.split(",")(0).toInt
      val name = info.split(",")(1)
      val age = info.split(",")(2).toInt
      val score = info.split(",")(3).toDouble
      Row(id, name, age, score)
    })
    val structType: StructType = StructType(Array[StructField](
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("score", DoubleType)
    ))
    val frame: DataFrame = session.createDataFrame(rowRDD,structType)
    frame.createTempView("tmp_person2")
    session.sql("select * from tmp_person2 ").show()
  }
}

/**
 * 样例类，封装数据
 * @param id
 * @param name
 * @param age
 * @param score
 */
case class MyPerson(id: Int, name: String, age: Int, score: Double)

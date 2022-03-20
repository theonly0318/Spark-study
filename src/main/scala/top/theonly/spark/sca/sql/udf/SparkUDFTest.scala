package top.theonly.spark.sca.sql.udf

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * UDF：User Defined Function，用户自定义函数
 *    session.udf.register("funcName", (arg)=> {...})
 *    session.udf.register("funcName", (arg1, arg2)=> {...})
 */
object SparkUDFTest {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkUDFTest")
      .getOrCreate()

    // 一些数据
    val list: List[String] = List[String]("zhangsan", "lisi", "wangwu", "zhaoliu")

    import session.implicits._

    // 转换为DataFrame
    val frame: DataFrame = list.toDF("name")
    // 创建临时视图
    frame.createTempView("a")

    /**
     * 自定义函数 len
     */
    session.udf.register("len", (name: String)=> name.length)
    // 在sql中使用自定义函数UDF
    val frame1: DataFrame = session.sql("select name, len(name) from a")
    frame1.show()

    /**
     * 自定义函数 len2
     */
    session.udf.register("len2", (s1: String, s2: String)=> s1.length+"##"+s2)
    // 在sql中使用自定义函数UDF
    val frame2 = session.sql(
      """
        |select name, len2(name, name) from a
        |""".stripMargin)
    frame2.show()
  }

}

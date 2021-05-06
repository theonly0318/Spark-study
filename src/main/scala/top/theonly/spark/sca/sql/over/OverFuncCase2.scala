package top.theonly.spark.sca.sql.over

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * over() 开窗函数 案例1：
 *
 * 求每个用户每天的累积的登录时长
 *
 * uerId     date    duration     |     sum(duration)
 *    1   2020-11-01  1           |      1
 *    1   2020-11-02  2           |      3
 *    1   2020-11-03  3           |      6
 *    2   2020-11-01  4           |      4
 *    2   2020-11-02  5           |      9
 *    2   2020-11-03  6           |      15
 *    3   2020-11-01  7           |      7
 *    3   2020-11-02  8           |      15
 *    3   2020-11-03  9           |      24
 *    4   2020-11-02  10          |      10
 *    4   2020-11-03  11          |      21
 */

object OverFuncCase2 {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkReadCsvTest")
      .getOrCreate()

    import session.implicits._

    val d: Dataset[String] = session.read.textFile("./data/sql/user_acc_info.txt")


    val ds: Dataset[UserAccInfo] = d.map(line => {
      val split: Array[String] = line.split(" ")
      UserAccInfo(split(0).toLong, split(1).toString, split(2).toLong)
    })
    val frame: DataFrame = ds.toDF()
    frame.createTempView("tmp")

    val frame1 = session.sql(
      """
        |select id, date, duration, sum(duration) over(partition by id order by date) sum_duration
        |from tmp
        |order by id, sum_duration
        |""".stripMargin)
    frame1.show()

  }
}

case class UserAccInfo(id: Long, date: String, duration: Long)



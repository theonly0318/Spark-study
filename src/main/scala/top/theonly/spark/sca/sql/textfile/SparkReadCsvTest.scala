package top.theonly.spark.sca.sql.textfile

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取csv文件
 *  csv文件：由逗号分隔的文件
 */
object SparkReadCsvTest {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkReadCsvTest")
      .getOrCreate()

//    val frame: DataFrame = session.read
//      // 有列头
//      .option("header", true)
//      .format("csv")
//      .load("./data/sql/test.csv")

    val frame: DataFrame = session.read
      // 有列头
      .option("header", true)
      .csv("./data/sql/test.csv")

    frame.show()
  }

}

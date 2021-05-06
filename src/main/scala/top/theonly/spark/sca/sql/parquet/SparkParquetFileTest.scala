package top.theonly.spark.sca.sql.parquet

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 读取json文件，并将DataFrame保存为parquet文件
 *
 * 保存成parquet文件的方式有两种：
 *
 *    1、df.write.mode(SaveMode.Overwrite).parquet("path")
 *    2、df.write.mode(SaveMode.ErrorIfExists).format("parquet").save("path")
 *
 * 读取parquet文件：
 *    val df: DataFrame = session.read.parquet("path")
 *    val df2: DataFrame = session.read.format("parquet").load("path")
 */
object SparkParquetFileTest {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkParquetFileTest")
      .getOrCreate()
    val df: DataFrame = session.read.json("./data/sql/json")
    df.show()

    /**
     * mode：指定文件保存时的模式
     *      SaveMode.Append：追加
     * 默认 SaveMode.ErrorIfExists：如果文件目录已存在，报错
     *      SaveMode.Ignore：如果存在就忽略
     *      SaveMode.Overwrite：覆盖
     */
    df.write.mode(SaveMode.Overwrite).parquet("./data/sql/parquet")
//    df.write.mode(SaveMode.ErrorIfExists).format("parquet").save("./data/sql/parquet")

    val df2: DataFrame = session.read.parquet("./data/sql/parquet")
//    val df2: DataFrame = session.read.format("parquet").load("./data/sql/parquet")
    df2.show()
  }

}

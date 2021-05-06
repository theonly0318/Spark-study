package top.theonly.spark.sca.sql.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 从mysql数据库读取数据，创建临时视图，使用sql查询，并将结果写入到MySQL中
 */
object JdbcTempViewTest {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local")
      // 默认为200，在Executor端会启动200个Task，如果数据量很小，要设置的小一些
      // sql中有join，group或聚合时
      .config("spark.sql.shuffle.partitions", "1")
      .appName("ReadJdbcTest")
      .getOrCreate()

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    val stuFrame: DataFrame = session.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_test", "tb_student", properties)
    val scoreFrame: DataFrame = session.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_test", "tb_score", properties)

    // 创建临时视图
    stuFrame.createTempView("stu_view")
    scoreFrame.createTempView("score_view")

    // sql
    val frame: DataFrame = session.sql(
      """
        |select stu_view.id, stu_view.name, stu_view.age, score_view.score
        |from stu_view, score_view where stu_view.id = score_view.stu_id
        |""".stripMargin)
    frame.show()

    // 存入Mysql，如果表不存在，spark会自动创建（创建表时，要注意主键约束、唯一约束等）
    frame.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://127.0.0.1:3306/spark_test", "tb_result", properties)
  }

}

package top.theonly.spark.sca.sql.jdbc

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOnJdbcTest {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local")
      .appName("SparkOnJdbcTest")
      .getOrCreate()

    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")

    /**
     * 第一种方式
     *  如果要多表联合查询：例如：select tp.id, tp.name, tp.age, ts.score from tb_student tp left join tb_score ts on tp.id = ts.stu_id
     *    可以将 table参数设置为  (select tp.id, tp.name, tp.age, ts.score from tb_student tp left join tb_score ts on tp.id = ts.stu_id) T
     */
    val df: DataFrame = session.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_test", "tb_student", properties)
    df.show()

    val df1: DataFrame = session.read.jdbc("jdbc:mysql://127.0.0.1:3306/spark_test", "(select tp.id, tp.name, tp.age, ts.score from tb_student tp left join tb_score ts on tp.id = ts.stu_id) T", properties)
    df1.show()

    val map: Map[String, String] = Map[String, String](
      "url" -> "jdbc:mysql://127.0.0.1:3306/spark_test",
      "user" -> "root",
      ("password", "123456"),
      "dbtable" -> "tb_student"
    )

    /**
     * 第二种方式
     */
    val df2: DataFrame = session.read.format("jdbc").options(map).load()
    df2.show()

    /**
     * 第三种方式
     */
    val df3: DataFrame = session.read.format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/spark_test")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "tb_student")
      .load()
    df3.show()


  }
}

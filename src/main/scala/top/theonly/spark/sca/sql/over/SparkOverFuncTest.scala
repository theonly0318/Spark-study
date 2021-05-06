package top.theonly.spark.sca.sql.over


import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * over() 开窗函数
 *
 * 注意：
 *    row_number() 开窗函数是按照某个字段分组，然后取另一字段的前几个的值，相当于 分组取topN
 *
 *    如果SQL语句里面使用到了开窗函数，那么这个SQL语句必须使用HiveContext来执行，HiveContext默认情况下在本地无法创建。在MySql8之后也增加了开窗函数。
 *
 * 开窗函数格式：
 *    row_number() over(partition by XXX order by XXX)
 *
 *
 * hive 表数据
 *  日期 类别 金额
 *    1   A   1
 *    1   A   2
 *    1   B   3
 *    1   C   4
 *    1   A   5
 *    1   B   6
 *    1   C   7
 *    1   C   8
 *    1   B   9
 *    1   A   10
 *    2   D   11
 *    2   D   12
 *    2   D   13
 * 求每个类别每日金额前三的数据，要求显示 日期 类别 金额
 */
object SparkOverFuncTest {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      // 本地运行，要配置Hive metastore的 thrift地址（hive-site.xml配置文件中的）
      // 加载数据的文件地址也要修改为本地，不建议使用本地模式（数据大时会崩溃）
      //      .master("local").config("hive.metastore.uris", "thrift://node1:9083")
      .appName("SparkOverFuncTest")
      // 开启hive支持
      .enableHiveSupport()
      .getOrCreate()

    session.sql("use db_spark")

    session.sql(
      """
        |create table if not exists tb_sales (
        |  riqi string,
        |  leibie string,
        |  jine Int
        |)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    session.sql("load data local inpath '/root/spark/test/sales.txt' into table tb_sales")

    val result = session.sql(
      """
        |select t.riqi, t.leibie, t.jine
        |from (
        |  select riqi,leibie,jine,row_number() over(partition by leibie order by jine desc) rank from tb_sales
        |) t
        |where t.rank<=3
        |""".stripMargin)
    result.write.mode(SaveMode.Append).saveAsTable("salesResult")
    result.show(100)
  }

}

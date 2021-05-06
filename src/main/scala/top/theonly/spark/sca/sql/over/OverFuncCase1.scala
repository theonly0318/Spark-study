package top.theonly.spark.sca.sql.over

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * over() 开窗函数 案例1：
 *
 * 数据
 *  id,channel,name
 *  1,channel-1,a
 *  1,channel-1,b
 *  1,channel-1,c
 *  1,channel-2,d
 *  1,channel-2,e
 *  1,channel-1,f
 *  2,channel-2,g
 *  2,channel-2,h
 *  2,channel-1,i
 *  2,channel-1,j
 *  2,channel-2,k
 *
 * 将每组相同id的每上下相邻的两行比较，如果channel发生了改变，输出改变的这一行
 *
 *
 *   id channel name rank
 *    1 channel-1 a 1         1 channel-1 b 2
 *    1 channel-1 b 2         1 channel-1 c 3
 *    1 channel-1 c 3         1 channel-2 d 4
 *    1 channel-2 d 4         1 channel-2 e 5
 *    1 channel-2 e 5         1 channel-1 f 6
 *    1 channel-1 f 6
 */
object OverFuncCase1 {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkReadCsvTest")
      .getOrCreate()

    val frame: DataFrame = session.read
      // 有列头
      .option("header", true)
      .csv("./data/sql/test.csv")
    frame.createTempView("aaa")

    session.sql(
      """
        |select id, channel, name, row_number() over(partition by id order by name) as rank
        |from aaa
        |""".stripMargin).createTempView("over_tmp")

    val resFrame = session.sql(
      """
        |select tmp2.id, tmp2.channel, tmp2.name
        | from over_tmp tmp1, over_tmp tmp2
        | where tmp1.id = tmp2.id
        |  and tmp1.rank = tmp2.rank-1
        |  and tmp1.channel != tmp2.channel
        |""".stripMargin)
    resFrame.show()
  }

}

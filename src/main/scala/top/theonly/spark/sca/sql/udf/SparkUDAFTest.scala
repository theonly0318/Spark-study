package top.theonly.spark.sca.sql.udf

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * UDAF：User Defined Aggregate Function，用户自定义聚合函数
 *
 *  session.udf.register("funcName", new UserDefinedAggregateFunction{...} )
 *
 *  例如：max，min，sum，avg，count
 *
 *  语句和函数同时出现在select后的普通字段，都要出现在group by后面
 *      select count(name) from tb_student
 *      select age, count(name) from tb_student group by age
 */
object SparkUDAFTest {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkUDAFTest")
      .getOrCreate()

    // 一些数据
    val list: List[String] = List[String](
      "zhangsan", "lisi", "zhaoliu",
      "zhangsan", "wangwu", "zhaoliu",
      "zhangsan", "lisi", "wangwu",
      "lisi", "wangwu", "zhaoliu",
      "zhangsan", "lisi", "wangwu"
    )

    import session.implicits._
    val frame: DataFrame = list.toDF("name")
    frame.createTempView("aaa")

    session.udf.register("mycount", new UserDefinedAggregateFunction {

      /**
       * 指定UDAF参数类型
       * @return
       */
      override def inputSchema: StructType = StructType(List[StructField](
        StructField("name", DataTypes.StringType, true)
      ))

      /**
       * 处理数据过程中更新的数据类型
       * @return
       */
      override def bufferSchema: StructType = StructType(List[StructField](
        StructField("count", DataTypes.IntegerType, true)
      ))

      /**
       * 指定evaluate 方法返回结构的类型
       * @return
       */
      override def dataType: DataType = DataTypes.IntegerType

      /**
       * 每次运行处理数据的顺序，true：保持一致
       * @return
       */
      override def deterministic: Boolean = true

      /**
       * map端：作用在每个分区的每个分组上，给每个分区每个分组初始值
       * reduce端：给每个分组做初始值
       * @param buffer
       */
      override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, 0)

      /**
       * 对每个分区的每个分组 更新值
       * @param buffer
       * @param input sql语句中UDAF函数的参数
       *                mycount(name)， input就是name的值, name = input.get(0)
       *                mycount(name, 10, age)， name = input.get(0), 10 = input.get(1), age = input.get(2)
       */
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        val name: String = input.getString(0)
        buffer.update(0, buffer.getInt(0)+1)
      }

      /**
       * 合并各个分区的相同分组的数据 （shuffle）
       * @param buffer1 初始化值(0，0)
       * @param buffer2 zhangsan=2  (0,2)
       */
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1.update(0, buffer1.getInt(0) + buffer2.getInt(0))
      }

      /**
       * 获取最终数据结果
       * @param buffer
       * @return
       */
      override def evaluate(buffer: Row): Any = buffer.getInt(0)
    })

    val frame1 = session.sql("select name, mycount(name) as count from aaa group by name")
    frame1.show()

  }

}

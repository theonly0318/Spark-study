package top.theonly.spark.sca.sql.textfile

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkReadJsonTest {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local")
      .appName("SparkReadCsvTest")
      .getOrCreate()

     val frame: DataFrame = session.read.json("./data/sql/json")
//    val frame: DataFrame = session.read.format("json").load("./data/json")

//    // 打印数据
//    frame.show()
//    // 打印Schema信息
//    frame.printSchema()

    /**
     * DataFrame API 操作
     */
    // select name ,age from table
    frame.select("name", "age").show()

    // select name, (age+10) as addage from table
    frame.select(frame.col("name"), frame.col("age").plus(10).as("addage")).show()

    // select name, age from table where age >= 25
//    frame.select("name", "age").where(frame.col("age")>=25).show()
    frame.select("name", "age").where(frame.col("age").>=(25)).show()
//    frame.filter(frame.col("age") .>= (25)).show()
//    frame.filter(frame.col("age") >= 25).show()
//    frame.filter("age >= 25").show()

    // select id, name, age from table order by name asc, age desc
    import session.implicits._
    frame.sort($"name".asc, frame.col("age").desc).show()

    // select id, name, age from table where age is not null
    frame.filter("age is not null").show()
//    frame.filter(frame.col("age").isNotNull).show()

    /**
     * 创建临时表
     */
    frame.createTempView("tmp")
//    frame.createGlobalTempView("tmp")
//    frame.createOrReplaceTempView("tmp")
//    frame.createOrReplaceGlobalTempView("tmp")
    // 使用sql查询
    session.sql("select name, age from tmp where age >= 25").show()

    /**
     * 将DataFrame转成JavaRDD
     * 注意：
     * 1.可以使用row.getInt(0),row.getString(1)...通过下标获取返回Row类型的数据，但是要注意列顺序问题---不常用
     * 2.可以使用row.getAs("列名")来获取对应的列值。
     */
    val rdd = frame.rdd
    rdd.foreach(row=>{
//      println(row)
      val id = row.getAs[Long]("id")
      val name = row.getAs[String]("name")
      val age = row.getAs[Long]("age")
      println(s"{id:$id, name: $name, age: $age}")
    })


  }

}

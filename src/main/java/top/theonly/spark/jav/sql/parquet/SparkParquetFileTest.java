package top.theonly.spark.jav.sql.parquet;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

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
 *
 * @author  theonly
 */
public class SparkParquetFileTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("SparkParquetFileTest")
                .getOrCreate();

        Dataset<Row> ds = session.read().json("./data/sql/json");
        // 展示数据
        ds.show();

        /**
         * mode：指定文件保存时的模式
         *      SaveMode.Append：追加
         * 默认 SaveMode.ErrorIfExists：如果文件目录已存在，报错
         *      SaveMode.Ignore：如果存在就忽略
         *      SaveMode.Overwrite：覆盖
         */
        // 将数据存储为 parquet文件
        ds.write().mode(SaveMode.Overwrite)
                .parquet("./data/sql/parquet_java");
//        ds.write().mode(SaveMode.Overwrite)
//                .format("parquet")
//                .save("./data/sql/parquet_java");

        // 读取parquet文件
        Dataset<Row> ds2 = session.read().parquet("./data/sql/parquet_java");
        ds2.show();

    }
}

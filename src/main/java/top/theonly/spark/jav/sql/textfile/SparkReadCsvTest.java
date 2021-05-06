package top.theonly.spark.jav.sql.textfile;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 读取csv文件
 *      用逗号,分隔数据的文件
 */
public class SparkReadCsvTest {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder().master("local").appName("SparkReadJsonTest").getOrCreate();
        Dataset<Row> ds = session.read()
                // 由列头名
                .option("header", true)
                .csv("./data/sql/test.csv");
        ds.show();

    }
}

package top.theonly.spark.jav.sql.udf;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

/**
 * UDF：User Defined Function，用户自定义函数
 *    session.udf.register("funcName", (arg)=> {...}, DataType)
 *    session.udf.register("funcName", (arg1, arg2)=> {...}, DataType)
 */
public class SparkUDFTest {

    public static void main(String[] args) {

        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("SparkUDFTest")
                .getOrCreate();
        // 读取json文件创建Dataset
        Dataset<Row> ds = session.read().json("./data/sql/json");
        // 注册临时表
        ds.registerTempTable("tmp");

        // 注册udf 自定义函数 len
//        session.udf().register("len", s->s.toString().length(), DataTypes.IntegerType);
//        session.udf().register("len", (String s)->s.length(), DataTypes.IntegerType);
        session.udf().register("len", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        // 注册udf 自定义函数 len2
//        session.udf().register("len2", (String s1, String s2) -> s1.length()+"#"+s2, DataTypes.StringType);
        session.udf().register("len2", new UDF2<String, String, String>() {
            @Override
            public String call(String s1, String s2) throws Exception {
                return s1.length() + "#" + s2;
            }
        }, DataTypes.StringType);


        Dataset<Row> dataset = session.sql("select name, len(name) len from tmp");
        dataset.show();

        Dataset<Row> dataset2 = session.sql("select name, len2(name, name) len2 from tmp");
        dataset2.show();


    }
}

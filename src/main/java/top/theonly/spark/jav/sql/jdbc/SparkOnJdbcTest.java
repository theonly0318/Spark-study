package top.theonly.spark.jav.sql.jdbc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SparkOnJdbcTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                // 默认为200，在Executor端会启动200个Task，如果数据量很小，要设置的小一些
                // sql中有join，group或聚合时
//                .config("spark.sql.shuffle.partitions", "1")
                .master("local")
                .appName("SparkOnJdbcTest")
                .getOrCreate();


        /**
         * 第一种方式 session.read().jdbc(url, tableName, properties)
         */
//        Properties properties = new Properties();
//        properties.setProperty("user", "root");
//        properties.setProperty("password", "123456");
//
//        Dataset<Row> dataset = session.read().jdbc("jdbc:mysql://localhost:3306/spark_test", "tb_student", properties);
//        dataset.show();

        /**
         * 第二种方式 session.read().format("jdbc").options(map).load()
         */
//        Map<String, String> map = new HashMap<>();
//        map.put("url", "jdbc:mysql://localhost:3306/spark_test");
//        map.put("user", "root");
//        map.put("password", "123456");
//        map.put("dbtable", "tb_student");
//        Dataset<Row> dataset1 = session.read().format("jdbc").options(map).load();
//        dataset1.show();

        /**
         * 第二种方式 session.read().format("jdbc")
         *                      .option(key1, value1)
         *                      .option(key2, value2)
         *                      .option(key3, value3)
         *                      .load()
         */
//        Dataset<Row> dataset2 = session.read().format("jdbc")
//                .option("url", "jdbc:mysql://localhost:3306/spark_test")
//                .option("user", "root")
//                .option("password", "123456")
//                .option("dbtable", "tb_student")
//                .load();
//        dataset2.show();

        jdbcJoinQuery(session);

    }

    /**
     * 多表联合查询
     * select tp.id, tp.name, tp.age, ts.score from tb_student tp left join tb_score ts on tp.id = ts.stu_id
     *
     * 方式1：把这条sql语句作为一张表，给个别名
     *
     * 方式2：创建视图
     * @param session
     */
    public static void jdbcJoinQuery(SparkSession session) {
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123456");

        // 方式1
        Dataset<Row> dataset = session.read().jdbc("jdbc:mysql://localhost:3306/spark_test",
                "(select tp.id, tp.name, tp.age, ts.score from tb_student tp left join tb_score ts on tp.id = ts.stu_id) T",
                properties);
//        dataset.show();

        // 方式2
        Dataset<Row> stuDs = session.read().jdbc("jdbc:mysql://localhost:3306/spark_test",
                "tb_student",
                properties);
        Dataset<Row> scoDs = session.read().jdbc("jdbc:mysql://localhost:3306/spark_test",
                "tb_score",
                properties);

        stuDs.registerTempTable("view_stu");
        scoDs.registerTempTable("view_sco");

        String sqlText = "select vt.id, vt.name, vt.age, vs.score from view_stu vt, view_sco vs where vt.id = vs.stu_id ";
        Dataset<Row> result = session.sql(sqlText);
        result.show();

    }

    /**
     * 将Dataset写入到jdbc（MySQL）
     * @param dataset
     * @param url
     * @param tableName
     * @param properties
     */
    public static void write2Jdbc(Dataset<Row> dataset, String url, String tableName, Properties properties) {
        dataset.write().jdbc(url, tableName, properties);
    }

}

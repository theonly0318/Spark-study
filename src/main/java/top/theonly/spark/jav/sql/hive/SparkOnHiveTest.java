package top.theonly.spark.jav.sql.hive;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Spark on Hive
 *
 *  创建表，加载数据，查询，将结果保存到hive表中
 *
 *  package，上传jar包至Spark客户端，提交：
 *    spark-submit --master spark://[master1节点:port, master2节点:port] --class top.theonly.spark.sca.sql.hive.SparkOnHiveTest jar包位置
 *
 * 读取hive表数据
 *    val df: DataFrame = session.table("表名")
 *
 * 将数据保存到hive中
 *    frame.write.mode(SaveMode.Overwrite).saveAsTable("表名")
 *
 * @author theonly
 */
public class SparkOnHiveTest {
    public static void main(String[] args) {
        // 老版本是创建HiveContext（SqlContext的子类），之后都封装为了SparkSession
        SparkSession session = SparkSession.builder()
                // 本地运行，要配置Hive metastore的 thrift地址（hive-site.xml配置文件中的）
                // 加载数据的文件地址也要修改为本地，不建议使用本地模式（数据大时会崩溃）
//              .master("local").config("hive.metastore.uris", "thrift://node1:9083")
                .appName("SparkOnHiveTest")
                // enableHiveSupport() ： 开启Hive支持
                .enableHiveSupport()
                .getOrCreate();
        // 切换数据库
        session.sql("use db_spark");

        // 创建表tb_student
        session.sql("create table if not exists tb_student(" +
                "id int, name String, age int) " +
                "row format delimited fields terminated by '\t' ");
        // 从student.txt文件中加载数据到tb_student表中
        session.sql("load data local inpath '/root/spark/test/student.txt' into table tb_student");

        // 读取tb_student表数据
        Dataset<Row> ds = session.table("tb_student");
        ds.show(10);

        // 创建表tb_score
        session.sql("create table if not exists tb_score(id int, stu_id int, score int) " +
                "row format delimited fields terminated by '\t'");
        // 从score.txt文件中加载数据到tb_score表中
        session.sql("load data local inpath '/root/spark/test/score.txt' into table tb_score");

        // 两张表联合查询
        Dataset<Row> result = session.sql(
                "select stu.id, stu.name, stu.age, sco.score " +
                        "from tb_student stu, tb_score sco " +
                        "where stu.id = sco.stu_id ");
        // 将结果保存到hive的result表中（hive会自动创建，也可以手动创建）
        result.write().mode(SaveMode.Overwrite).saveAsTable("result");

    }
}

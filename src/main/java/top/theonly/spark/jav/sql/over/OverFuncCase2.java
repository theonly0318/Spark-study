package top.theonly.spark.jav.sql.over;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * over() 开窗函数 案例1：
 *
 * 求每个用户每天的累积的登录时长
 *
 * uerId     date    duration     |     sum(duration)
 *    1   2020-11-01  1           |      1
 *    1   2020-11-02  2           |      3
 *    1   2020-11-03  3           |      6
 *    2   2020-11-01  4           |      4
 *    2   2020-11-02  5           |      9
 *    2   2020-11-03  6           |      15
 *    3   2020-11-01  7           |      7
 *    3   2020-11-02  8           |      15
 *    3   2020-11-03  9           |      24
 *    4   2020-11-02  10          |      10
 *    4   2020-11-03  11          |      21
 *
 * @author theonly
 */
public class OverFuncCase2 {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("OverFuncCase1")
                .getOrCreate();
        // 读取文件获取Dataset
        Dataset<String> ds1 = session.read().textFile("./data/sql/user_acc_info.txt");
        // 转为rdd，要做列名
        JavaRDD<String> rdd = ds1.javaRDD();
        // map
        JavaRDD<UserAccInfo> mapRDD = rdd.map(line -> {
            String[] split = line.split(" ");
            Long id = Long.valueOf(split[0]);
            String date = split[1];
            Long duration = Long.valueOf(split[2]);
            return new UserAccInfo(id, date, duration);
        });
        // 根据rdd创建Dataset
        Dataset<Row> ds2 = session.createDataFrame(mapRDD, UserAccInfo.class);
//        ds2.show(10);
        // 注册临时表
        ds2.registerTempTable("tmp");
        // 查询，sum(duration) + 开窗函数over()
        Dataset<Row> ds3 = session.sql("select " +
                "id, date, duration, sum(duration) over(partition by id order by date) sum_duration " +
                "from tmp order by id, sum_duration");
        // 展示结果
        ds3.show();
    }
}




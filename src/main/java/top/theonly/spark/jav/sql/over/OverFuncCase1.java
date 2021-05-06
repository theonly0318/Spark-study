package top.theonly.spark.jav.sql.over;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
 *
 * @author theonly
 */
public class OverFuncCase1 {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("OverFuncCase1")
                .getOrCreate();

        // 读取数据文件，获取Dataset
        Dataset<Row> ds1 = session.read().option("header", true).csv("./data/sql/test.csv");
//        ds1.show();
        // 注册临时表
        ds1.registerTempTable("tmp1");
        // 开窗函数对数据记录行数
        Dataset<Row> ds2 = session.sql("select id, channel, name, row_number() over(partition by id order by name) as rank from tmp1");
        // 注册临时表
        ds2.registerTempTable("tmp2");
        // 查询，将两个tmp2临时表联合查询，条件：id相同，channel不同，表2的行数要比表1的行数+1
        Dataset<Row> ds3 = session.sql("select t2.id, t2.channel, t2.name " +
                "from tmp2 t1, tmp2 t2 " +
                "where t1.id = t2.id " +
                "and t1.rank = t2.rank-1 " +
                "and t1.channel != t2.channel " +
                "order by t2.id");
        ds3.show();
    }
}

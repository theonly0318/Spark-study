package top.theonly.spark.jav.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 多行数据转为一行
 * 一行数据转为多行
 *
 * 数据：
 *
 *      username item price
 *       zhangsan A   1
 *       zhangsan	B	2
 *       zhangsan	C	3
 *       lisi	A	4
 *       lisi	C	5
 *       zhangsan	D	6
 *       lisi	B	7
 *       wangwu	C	8
 *
 *  (1)
 *       username    items    total_price
 *       zhangsan   [A,B,C,D]   12
 *       lisi       [A,C,B] 	16
 *       wangwu 	[C]	        8
 *
 *  (2) 行转列
 *      username    A   B   C   D
 *      zhangsan    1   2   3   6
 *      lisi        4   7   5   0
 *      wangwu      0   0   8   0
 *
 *  (3) 列转行，将(2)的结果还原为原始模样
 *
 * @author theonly
 */
public class RowColumnTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("OverFuncCase1")
                .getOrCreate();
        // 读取文件获取Dataset
        Dataset<Row> ds1 = session.read().option("header", true).csv("./data/sql/row_column.csv");
        ds1.registerTempTable("tmp");

        /**
         * (1)
         */
//        func1(session);

        /**
         * (2)
         */
        row2Column(session);

        column2Row(session);
    }

    /**
     * (1)
     * @param session
     */
    public static void func1(SparkSession session) {
        /**
         * collect_list(col)：按照分组将元素收集到list集合中，可重复
         * collect_set(col)：按照分组将元素收集到list集合中，不可重复
         * concat('连接符',col1,col2...)：拼接多个字段
         * concat_ws('连接符',list)：将集合中的元素用指定连接符连接
         */
//        String sql = "select username, collect_list(item) items, sum(price) total_price " +
//                "from tmp group by username";
        String sql1 = "select username, concat_ws('#', collect_list(item)) items, sum(price) total_price " +
                "from tmp group by username";
        Dataset<Row> ds2 = session.sql(sql1);
        ds2.show();
        ds2.registerTempTable("tmp2");

        /**
         * explode(list)：将集合展开为多个元素
         * split(col, '连接符')：将列按照指定的连接符切割为list集合
         */
        String sql2 = "select username, explode(split(items, '#')) item, total_price from tmp2";
        Dataset<Row> ds3 = session.sql(sql2);
        ds3.show();
    }

    /**
     * (2) 行转列
     * 传统方式：
     * 1、join   如果join中的select和where后面的字段的值在表中有不存在，不能使用，本例中就不能使用
     * 2、union all
     * 3、DECODE(oracle)
     * 4、case when
     * @param session
     */
    public static void row2Column(SparkSession session) {
        /**
         * mysql 运行成功，spark失败
         */
//        String sql = "select t.username, sum(t.A) A, sum(t.B) B, sum(t.C) C, sum(t.D) D from (" +
//                "select username, price as 'A', 0 as 'B', 0 as 'C', 0 as 'D' from tmp where item = 'A' UNION ALL" +
//                "select username, 0 as 'A', price as 'B', 0 as 'C', 0 as 'D' from tmp where item = 'B' UNION ALL" +
//                "select username, 0 as 'A', 0 as 'B', price as 'C', 0 as 'D' from tmp where item = 'C' UNION ALL" +
//                "select username, 0 as 'A', 0 as 'B', 0 as 'C', price as 'D' from tmp where item = 'D'" +
//                ") t group by t.username";

        /**
         * str_to_map(str, '元素分隔符', 'kv分隔符')：将字符串转成map
         * A    1
         * B    2
         * C	3     =>      A:1  B:2  C:3   =>    [A:1,B:2,C:3]   =>   A:1#B:2#C:3    =>  [A -> 1, B -> 2, C -> 3]
         */
        String sql = "select username, str_to_map(concat_ws('#', collect_list(concat(item, ':', price))), '#', ':') as mp from tmp group by username";
        Dataset<Row> ds = session.sql(sql);
//        ds.show();
        ds.registerTempTable("a");

        String sql1 = "select username, mp['A'] as A, mp['B'] as B, mp['C'] as C, mp['D'] as D from a";
        Dataset<Row> ds2 = session.sql(sql1);
        /*
        +--------+---+---+---+---+
        |username|  A|  B|  C|  D|
        +--------+---+---+---+---+
        |  wangwu|  0|  0|  8|  0|
        |zhangsan|  1|  2|  3|  6|
        |    lisi|  4|  7|  5|  0|
        +--------+---+---+---+---+
         */
        ds2.show();
        ds2.registerTempTable("c");
    }

    /**
     * 列转行
     * username    A   B   C   D
     * zhangsan    1   2   3   6   =>   zhangsan    [A->1,B->2,C->3,D->6]   =>
     *                  username    key     value
     *                  zhangsan    A       1
     *                  zhangsan    B       2
     *                  zhangsan    C       3
     *                  zhangsan    D       6
     *   去除value为nul的数据
     * @param session
     */
    public static void column2Row(SparkSession session) {
//        String sql = "select username, str_to_map(concat_ws('#',collect_list(" +
//                "concat('A', ':', A), concat('B', ':', B), concat('C', ':', C), concat('D', ':', D))), '#', ':')  from c";
//        String sql = "select username, " +
//                "concat('A', ':', A), concat('B', ':', B), concat('C', ':', C), concat('D', ':', D) from c";
        String sql = "select username, explode(map('A', A, 'A', B, 'C', C, 'D', D)) from c";
        Dataset<Row> ds = session.sql(sql);
//        ds.show();
        ds.registerTempTable("d");
        String sql1 = "select username, key as item, value as price from d where value is not null";
        Dataset<Row> ds2 = session.sql(sql1);
        ds2.show();
    }
}

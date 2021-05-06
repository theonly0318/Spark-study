package top.theonly.spark.jav.sql.textfile;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import scala.Function1;
import scala.runtime.BoxedUnit;

/**
 * 1、创建SparkSession（也可以创建SqlContext）
 * 2、read数据文件
 *      java中返回的时DataSet<Row>对象， scala中返回的时 DataFrame对象
 */
public class SparkReadJsonTest {

    public static void main(String[] args) {

//        SparkConf conf = new SparkConf();
//        conf.setMaster("local").setAppName("SparkReadJsonTest");
//        SparkContext context = new SparkContext(conf);
//
//        //创建sqlContext
//        SQLContext sqlContext = new SQLContext(context);
//        Dataset<Row> ds = sqlContext.read().format("json").load("./data/sql/json");
//        ds.show();

        SparkSession session = SparkSession.builder().master("local").appName("SparkReadJsonTest").getOrCreate();
        Dataset<Row> dataset = session.read().json("./data/sql/json");
        // 打印数据
        dataset.show();
//      // 打印Schema信息
        dataset.printSchema();

        // DataSet<Row> api操作
//        dataSetApi(dataset);

        // 创建视图，并使用sql语句查询
//        createView(dataset, session);

        // 将DataSet<Row> 转为 JavaRDD
        dataset2JavaRDD(dataset);

    }

    /**
     * DataSet<Row> api操作（scala中时DataFrame）
     * @param ds
     */
    public static void dataSetApi(Dataset<Row> ds) {
        // select name, age from table
        ds.select("name", "age").show();

        // select name, (age+10) as addage from table
        ds.select(ds.col("name"), ds.col("age").plus(10).alias("addage")).show();

        // select name, age from table where age >= 25
        ds.select("name", "age").where(ds.col("age").geq(25)).show();
        ds.filter(ds.col("age").geq(25)).show();
        // select count(*) from table group by age
        ds.groupBy(ds.col("age")).count().show();

        // select id, name, age from table order by name asc, age desc
        ds.sort(ds.col("name").asc(), ds.col("age").desc()).show();

        ds.filter("age is not null").show();


    }

    /**
     * 创建视图
     * @param ds
     * @param session
     */
    public static void createView(Dataset<Row> ds, SparkSession session) {
        /**
         * 创建视图
         */
        ds.registerTempTable("tmp");
        String sqlText = "select name, age from tmp where age >= 25";
        Dataset<Row> d = session.sql(sqlText);
        d.show();

    }

    /**
     * 将DataSet<Row> 转为 JavaRDD
     *
     * 注意：
     *  1.可以使用row.getInt(0),row.getString(1)...通过下标获取返回Row类型的数据，
     *      但是要注意列顺序问题---不常用
     *  2.可以使用row.getAs("列名")来获取对应的列值。
     *
     *
     * @param ds
     */
    public static void dataset2JavaRDD(Dataset<Row> ds) {
        JavaRDD<Row> rdd = ds.javaRDD();

        rdd.foreach(row -> {
            Long id = row.getAs("id");
            String name = row.getAs("name");
            Long age = row.getAs("age");
            System.out.println("{id: "+id+", name: "+name+", age: "+age+"}");
        });


    }
}

package top.theonly.spark.jav.sql.textfile;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 通过RDD创建DataFrame
 *  1、json格式的RDD创建DataFrame
 *  2、非json格式的RDD创建DataFrame
 *      1). 通过反射的方式将非json格式的RDD转换成DataFrame（不建议使用）
 *           自定义类要可序列化
 *           自定义类的访问级别是Public
 *           RDD转成DataFrame后会根据映射将字段按Assci码排序
 *           将DataFrame转换成RDD时获取字段两种方式,
 *              一种是df.getInt(0)下标获取（不推荐使用），
 *              另一种是df.getAs(“列名”)获取（推荐使用）
 *
 *      2). 动态创建Schema将非json格式的RDD转换成DataFrame
 *
 * @author theonly
 */
public class CreateDataFrameByRddTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("CreateDataFrameByRddTest");
        JavaSparkContext context = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(context);
        /**
         * 1、通过json格式的RDD创建DataFrame
         */
        createDataFrameByJsonRDD(context, sqlContext);

        /**
         * 2、非json格式的RDD创建DataFrame
         * 通过反射的方式将非json格式的RDD转换成DataFrame（不建议使用）
         */
//        createDataFrameByRDDUseReflect(context, sqlContext);

        /**
         * 2、非json格式的RDD创建DataFrame
         * 动态创建Schema将非json格式的RDD转换成DataFrame
         */
//        createDataFrameByRDDByDynamicSchema(context, sqlContext);
    }

    /**
     * 通过json格式的RDD创建DataFrame
     * @param context
     * @param sqlContext
     */
    public static void createDataFrameByJsonRDD(JavaSparkContext context, SQLContext sqlContext) {
        List<String> list = new ArrayList<>();
        list.add("{'name':'zhangsan','age':'18'}");
        list.add("{'name':'lisi','age':'19'}");
        list.add("{'name':'wangwu','age':'20'}");
        list.add("{'name':'maliu','age':'21'}");
        list.add("{'name':'tainqi','age':'22'}");
        JavaRDD<String> rdd = context.parallelize(list);
        Dataset<Row> dataset = sqlContext.read().json(rdd);
        dataset.show();
    }

    /**
     * 非json格式的RDD创建DataFrame
     *      通过反射的方式将非json格式的RDD转换成DataFrame（不建议使用）
     * @param context
     * @param sqlContext
     */
    public static void createDataFrameByRDDUseReflect(JavaSparkContext context, SQLContext sqlContext) {
        JavaRDD<String> rdd = context.textFile("./data/sql/people.txt");
        JavaRDD<Person> personRDD = rdd.map(new Function<String, Person>() {
            @Override
            public Person call(String line) throws Exception {
                String[] split = line.split(",");
                Integer id = Integer.valueOf(split[0]);
                String name = split[1];
                Integer age = Integer.valueOf(split[2]);
                Double score = Double.valueOf(split[3]);
                return new Person(id, name, age, score);
            }
        });
        // 传入进去Person.class的时候，sqlContext是通过反射的方式创建DataFrame
        // 在底层通过反射的方式获得Person的所有field，结合RDD本身，就生成了DataFrame
        Dataset<Row> ds = sqlContext.createDataFrame(personRDD, Person.class);
        ds.show();
    }

    /**
     * 非json格式的RDD创建DataFrame       Row
     *      动态创建Schema将非json格式的RDD转换成DataFrame
     * @param context
     * @param sqlContext
     */
    public static void createDataFrameByRDDByDynamicSchema(JavaSparkContext context, SQLContext sqlContext) {
        JavaRDD<String> rdd = context.textFile("./data/sql/people.txt");
        // 转换成Row类型的RDD
        JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] split = line.split(",");
                Integer id = Integer.valueOf(split[0]);
                String name = split[1];
                Integer age = Integer.valueOf(split[2]);
                Double score = Double.valueOf(split[3]);
                return RowFactory.create(id, name, age, score);
            }
        });
        // 动态构建DataFrame中的元数据，一般来说这里的字段可以来源自字符串，也可以来源于外部数据库
        List<StructField> asList = Arrays.asList(
                DataTypes.createStructField("id", DataTypes.IntegerType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("score", DataTypes.DoubleType, true)
        );
        StructType structType = DataTypes.createStructType(asList);
        // sqlContext 创建DataFrame
        Dataset<Row> ds = sqlContext.createDataFrame(rowRDD, structType);
        ds.show();
    }
}

class Person implements Serializable {

    private Integer id;
    private String name;
    private Integer age;
    private Double score;

    public Person(Integer id, String name, Integer age, Double score) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.score = score;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }
}

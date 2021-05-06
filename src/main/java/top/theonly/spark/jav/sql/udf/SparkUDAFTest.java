package top.theonly.spark.jav.sql.udf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * UDAF：User Defined Aggregate Function，用户自定义聚合函数
 *
 *  session.udf.register("funcName", new UserDefinedAggregateFunction{...} )
 *
 *  例如：max，min，sum，avg，count
 *
 *  语句和函数同时出现在select后的普通字段，都要出现在group by后面
 *      select count(name) from tb_student
 *      select age, count(name) from tb_student group by age
 *
 * @author theonly
 */
public class SparkUDAFTest {

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local")
                .appName("SparkUDFTest")
                .getOrCreate();
        // 读取json文件创建Dataset
        Dataset<Row> ds = session.read().json("./data/sql/json");
        // 注册临时表
        ds.registerTempTable("tmp");

        session.udf().register("mycount", new UserDefinedAggregateFunction() {

            /**
             * 对每个分区的每个分组 更新值
             * @param buffer
             * @param input sql语句中UDAF函数的参数
             *                mycount(name)， input就是name的值, name = input.get(0)
             *                mycount(name, 10, age)， name = input.get(0), 10 = input.get(1), age = input.get(2)
             */
            @Override
            public void update(MutableAggregationBuffer buffer, Row input) {
                buffer.update(0, buffer.getInt(0)+1);
            }

            /**
             * 合并各个分区的相同分组的数据 （shuffle）
             * @param buffer1 初始化值(0，0)
             * @param buffer2 zhangsan=2  (0,2)
             */
            @Override
            public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
                buffer1.update(0, buffer1.getInt(0)+buffer2.getInt(0));
            }

            /**
             * 获取最终数据结果
             * @param buffer
             * @return
             */
            @Override
            public Object evaluate(Row buffer) {
                return buffer.getInt(0);
            }

            /**
             * map端：作用在每个分区的每个分组上，给每个分区每个分组初始值
             * reduce端：给每个分组做初始值
             * @param buffer
             */
            @Override
            public void initialize(MutableAggregationBuffer buffer) {
                buffer.update(0, 0);
            }

            /**
             * 指定udaf函数参数类型
             * @return
             */
            @Override
            public StructType inputSchema() {
                List<StructField> list = Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true));
                return DataTypes.createStructType(list);
            }

            /**
             * 处理数据过程中更新的数据类型
             * @return
             */
            @Override
            public StructType bufferSchema() {
                List<StructField> list = Arrays.asList(DataTypes.createStructField("count", DataTypes.IntegerType, true));
                return DataTypes.createStructType(list);
            }

            /**
             * 指定evaluate 方法返回结构的类型
             * @return
             */
            @Override
            public DataType dataType() {
                return DataTypes.IntegerType;
            }

            /**
             * 每次运行处理数据的顺序，true：保持一致
             * @return
             */
            @Override
            public boolean deterministic() {
                return true;
            }
        });

        Dataset<Row> dataset = session.sql("select age, mycount(name) as count from tmp group by age");
        dataset.show();
    }
}

package top.theonly.spark.jav.core.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 测试自定义累加器的使用
 * @author theonly
 */
public class SelfDefinedAccumulatorTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SecondSortCase");

        JavaSparkContext context = new JavaSparkContext(conf);

        // 模拟数据
        List<String> list = Arrays.asList("zhangsan,20,0","lisi,21,1","wangwu,22,0","zhaoliu,21,1",
                "tianqi,22,0","huangba,23,1","majiu,24,0","liushi,25,1");
        JavaRDD<String> rdd1 = context.parallelize(list, 3);

        // 获取Scala中的SparkContext
        SparkContext sc = context.sc();
        // 注册自定义的累加器
        MyAccumulator acc = new MyAccumulator();
        sc.register(acc, "myAcc");

        // map
        JavaRDD<String> map = rdd1.map(new Function<String, String>() {
            @Override
            public String call(String line) throws Exception {
                String[] split = line.split(",");
                Integer age = Integer.valueOf(split[1]);
                Integer gender = Integer.valueOf(split[2]);
                PersonInfo personInfo = null;
                if (gender == 0) {
                    // 封装PersonInfo对象，进来一条数据personCount为1,age,
                    personInfo = new PersonInfo(1, age, 0, 1);
                } else if (gender == 1) {
                    personInfo = new PersonInfo(1, age, 1, 0);
                }
                // 累加器相加
                acc.add(personInfo);
                return line;
            }
        });
        // action算子，触发执行
        map.count();
        // 获取自定义累加器的值
        PersonInfo info = acc.value();
        // PersonInfo{personCount=8, ageCount=178, maleCount=4, femaleCount=4}
        System.out.println(info);

    }
}

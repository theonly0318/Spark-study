package top.theonly.spark.jav.core.cases.secondsort;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 二次排序
 * 对secondsort.txt文件中的每一行进行排序（降序），先对第一个数字排序，再对第二个数字排序
 * @author theonly
 */
public class SecondSortCase {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("SecondSortCase");

        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> rdd = context.textFile("C:\\code\\IdeaWorkspace\\spark-code\\data\\secondsort.txt");

        JavaPairRDD<MySort, String> pairRDD = rdd.mapToPair(new PairFunction<String, MySort, String>() {
            @Override
            public Tuple2<MySort, String> call(String line) throws Exception {
                Integer firstNum = Integer.valueOf(line.split(" ")[0]);
                Integer secondNum = Integer.valueOf(line.split(" ")[1]);
                return new Tuple2<>(new MySort(firstNum, secondNum), line);
            }
        });

        JavaPairRDD<MySort, String> sortRDD = pairRDD.sortByKey(false);

        sortRDD.foreach(new VoidFunction<Tuple2<MySort, String>>() {
            @Override
            public void call(Tuple2<MySort, String> tp) throws Exception {
                System.out.println(tp._2);
            }
        });

    }
}

package top.theonly.spark.sca.MLlib.lr

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 线性回归
 *
 * @author theonly
 */
object LinearRegressionNewAPITest {

  def main(args: Array[String]): Unit = {
    // 获取SparkSession
    val session: SparkSession = SparkSession.builder().master("local").appName("LinearRegressionNewAPITest").getOrCreate()
    // 读取文件
    val training: DataFrame = session.read.format("libsvm").load("./data/MLlib/sample_linear_regression_data.txt")
    // 创建线性回归算法对象
    // setMaxIter(1000) 最大迭代次数
    // setRegParam(0.4) 设置步长
    // setElasticNetParam(0.8) 训练集
    val regression: LinearRegression = new LinearRegression().setMaxIter(1000).setRegParam(0.3).setElasticNetParam(0.8)
    // 训练模型
    val model = regression.fit(training)
    // coefficients：模型系数， intercept：模型截距
    println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

    // Summarize the model over the training set and print out some metrics
    // 总结整个训练集的模型并打印出一些指标
    val trainingSummary = model.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    //    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    // 展示误差
    trainingSummary.residuals.show()
    // 打印 均方根误差
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //    println(s"r2: ${trainingSummary.r2}")
  }
}

package top.theonly.spark.sca.MLlib.lr

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 线性回归
 *
 * @author theonly
 */
object LinearRegressionTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("LinearRegressionTest")
//    Logger.getRootLogger.setLevel(Level.WARN)
    val context = new SparkContext(conf)
    context.setLogLevel("ERROR")

    val data: RDD[String] = context.textFile("./data/MLlib/lpsa.data")

    val examples: RDD[LabeledPoint] = data.map(line => {
      val split: Array[String] = line.split(",")
      val y: String = split(0)
      val xs: String = split(1)
      LabeledPoint(y.toDouble, Vectors.dense(xs.split(" ").map(_.toDouble)))
    })

    val train2TestData: Array[RDD[LabeledPoint]] = examples.randomSplit(Array(0.8, 0.2), 1L)

    val lr = new LinearRegressionWithSGD()
    lr.setIntercept(true)
    lr.optimizer.setStepSize(0.2)
    lr.optimizer.setNumIterations(1000)
    lr.optimizer.setMiniBatchFraction(1)

    val model: LinearRegressionModel = lr.run(train2TestData(0))

//    val model: LinearRegressionModel = LinearRegressionWithSGD.train(train2TestData(0), 1000, 0.1, 1)
    println("weights = " +  model.weights)
    println("intercept = " + model.intercept)

    // 对样本进行测试
    val prediction: RDD[Double] = model.predict(train2TestData(1).map(_.features))
    val predictionAndLabel: RDD[(Double, Double)] = prediction.zip(train2TestData(1).map(_.label))

    val print_predict = predictionAndLabel.take(20)
    println("prediction" + "\t" + "label")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    // 计算测试集平均误差
    val loss = predictionAndLabel.map {
      case (p, v) =>
        val err = p - v
        Math.abs(err)
    }.reduce(_ + _)
    val error = loss / train2TestData(1).count
    println("Test RMSE = " + error)
    // 模型保存
    //    val ModelPath = "model"
    //    model.save(sc, ModelPath)
    //    val sameModel = LinearRegressionModel.load(sc, ModelPath)
    context.stop()

  }
}
